/**
 * ## Obter o Grafo De Bruijn
 * O objetivo deste exercício é, dado um conjunto de k-mers, obter o grafo de Bruijn. Por exemplo, se considerarmos o
 * seguinte conjunto de k-mers:
 *         TAA, AAT, ATG, TGC, GCC, CCA, CAT, ATG, TGG, GGG, GGA, GAT, ATG, TGT, GTT, TTA,
 * O grafo de Bruijn deve ter:
 * (vertices = adjacentes do vertice)
 * AA = [AT ]
 * CC = [CA ]
 * GG = [GG, GA]
 * TT = [TA ]
 * AT = [TG, TG, TG]
 * TG = [GC, GG, GT]
 * GA = [AT ]
 * GC = [CC ]
 * TA = [AA ]
 * GT = [TT ]
 * CA = [AT ]
 * Além deste pequeno exemplo, poderá testar este exercício utilizando os datasets reais disponíveis em
 *     https://kmer.pennstatehealth.net/kMerDB/downloads,
 * os ficheiros em anexo a esta série de problems, ou o 'output' do exercício 1.
 *
 * Cada linha do ficheiro de Result_1.txt contém um conjunto de k-mers, e o grafo de Bruijn é representado como um
 * dicionário onde cada chave é um prefixo e o valor é uma lista de sufixos adjacentes.
 * Os ficheiros temporários serão criados por cada thread disponível numa Pool. Todas as threads estarão a ler
 * o mesmo ficheiro de 'input', mas cada thread irá criar o seu próprio ficheiro temporário.
 * O 'output' para o novo ficheiro deverá ser a junção de todos os ficheiros temporários e a sua estrutura deverá ser:
 * AA>AT;CC>CA;GG>GG,GA;TT>TA;AT>TG,TG,TG;TG>GC,GG,GT;GA>AT;GC>CC;TA>AA;GT>TT;CA>AT;
 *
 */

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

// -----------------------------------------------------------------------------
// Tuning constants
// -----------------------------------------------------------------------------

private const val EX2_QUEUE_CAPACITY = 20
private const val EX2_BATCH_SIZE = 100_000

// -----------------------------------------------------------------------------
// Batch model + format tokens
// -----------------------------------------------------------------------------

private val EX2_POISON = LineBatch(-1, emptyList())


// -----------------------------------------------------------------------------
// Worker — processes one batch → one temp file with one graph line per input line
// -----------------------------------------------------------------------------

/**
 * Consumes a single [KmerLineBatch] from [queue] and, **for each input line**,
 * builds a local De Bruijn graph and writes **one output line** in the format:
 *
 *   `prefix>suf1,suf2,...;prefix2>...;`
 *
 * This preserves the 1-to-1 mapping between input lines (reads) and output lines
 * (per-read De Bruijn graphs).  Each temp file is named `{batchId}.ex2` so that
 * the merge step can concatenate them in the original read order.
 *
 * **No global state** is accumulated across lines — the per-line graph is tiny
 * (at most O(4^(k-1)) = 16 prefixes for k=3) and is allocated fresh each line.
 */
private fun buildDeBruijnWorker(
    queue: LinkedBlockingQueue<LineBatch>,
    tempFolder: String
) {
    while (true) {
        val batch = queue.take()
        if (batch.batchId == -1) break

        val tempFile = File(tempFolder, "${batch.batchId}.ex2")
        tempFile.bufferedWriter().use { writer ->
            // Per-line graph — reused buffer cleared each iteration
            val lineGraph = HashMap<String, MutableList<String>>(16)

            for (rawLine in batch.lines) {
                val line = rawLine.trim()
                if (line.isEmpty()) continue

                lineGraph.clear()

                // Parse comma-separated k-mers and populate the local graph
                var pos = 0
                while (pos < line.length) {
                    val commaIdx = line.indexOf(SEPARATOR, pos)
                    val end = if (commaIdx == -1) line.length else commaIdx
                    val kLen = end - pos
                    if (kLen >= 2) {
                        // prefix = kmer[0..kLen-2], suffix = kmer[1..kLen-1]
                        val prefix = line.substring(pos, pos + kLen - 1)
                        val suffix = line.substring(pos + 1, pos + kLen)
                        lineGraph.getOrPut(prefix) { mutableListOf() }.add(suffix)
                    }
                    pos = end + 1
                }

                if (lineGraph.isEmpty()) continue

                // Write one graph line: AA>AT;CC>CA;GG>GG,GA;...
                for ((prefix, suffixes) in lineGraph)
                    writer.write(prefix + CONNECTOR + suffixes.joinToString(DELIMITER.toString()) + SEPARATOR)

                writer.newLine()
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Main pipeline — streaming producer-consumer
// -----------------------------------------------------------------------------

/**
 * Builds a per-read De Bruijn graph from [fileInput] using a producer-consumer pipeline:
 *
 * 1. [numThreads] workers start and block on queue.
 * 2. The calling thread reads [fileInput] line by line, enqueues batches.
 * 3. Each worker processes its batch: one graph line written per input line.
 * 4. Temp files are concatenated in batch order → [fileOutput].
 *
 * Output: one line per read, format `prefix>suf1,suf2,...;prefix2>...;\n`
 */
fun buildDeBruijnGraph(
    fileInput: String,
    fileOutput: String,
    numThreads: Int = Runtime.getRuntime().availableProcessors(),
    tempFolder: String = "temporary_files/ex2/"
) {
    File(tempFolder).mkdirs()

    val queue = LinkedBlockingQueue<LineBatch>(EX2_QUEUE_CAPACITY)
    val executor = Executors.newFixedThreadPool(numThreads)

    val futures = (0..<numThreads).map {
        executor.submit { buildDeBruijnWorker(queue, tempFolder) }
    }
    executor.shutdown()

    println("---- Reading k-mer file: $fileInput")
    val linesRead = processFileBatches(fileInput, queue, EX2_BATCH_SIZE)
    println("---- Total lines read: $linesRead")

    // One poison pill per worker to signal end-of-stream
    repeat(numThreads) { queue.put(EX2_POISON) }
    futures.forEach { it.get() }
    println("---- Finished building per-read De Bruijn graphs.")

    mergeTempFiles(fileOutput, tempFolder, extension = ".ex2")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fun main() {
    val filePath   = "SRR494099.fastq.gz"
    val fileInput  = "results/Result_1_" + filePath.substring(0, filePath.length-9) + ".csv"
    val fileOutput = "results/Result_2_"+ filePath.substring(0, filePath.length-9) + ".csv"
    val numThreads = Runtime.getRuntime().availableProcessors()

    println("---- Using $numThreads threads (availableProcessors)")

    measureAndPrintTime("Finished building De Bruijn graph") {
        buildDeBruijnGraph(
            fileInput  = fileInput,
            fileOutput = fileOutput,
            numThreads = numThreads
        )
    }
}
