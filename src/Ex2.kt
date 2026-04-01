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
import kotlin.collections.iterator
import kotlin.text.substringBefore

// -----------------------------------------------------------------------------
// Tuning constants
// -----------------------------------------------------------------------------
private const val EX2_QUEUE_CAPACITY = 20
private const val EX2_BATCH_SIZE = 200_000

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
/**
 * Consumes batches and builds a local thread-specific De Bruijn graph.
 * Once all batches are processed, it outputs one file per vertex (prefix)
 * containing all suffixes found by this thread.
 */

private fun buildGlobalDeBruijnWorker(
    threadId: Int,
    queue: LinkedBlockingQueue<LineBatch>,
    tempFolder: String
) {
    // Prefix -> (Suffix -> Count)
    val localGraph = Pair<HashMap<String, HashSet<String>>, HashMap<String, HashMap<String,Int>>>(HashMap(), HashMap())

    while (true) {
        val batch = queue.take()
        if (batch.batchId == -1) break

        for (rawLine in batch.lines) {
            val line = rawLine.trim()
            if (line.isEmpty()) continue

            // Assuming input is from Ex1: ATC;TCG;CGA
            val kmers = line.split(SEPARATOR)
            for (kmer in kmers) {
                if (kmer.length < 2) continue
                val prefix = kmer.substring(0, kmer.length - 1)
                val suffix = kmer.substring(1)

                if (NEED_COUNT_4_Ex2) {
                    val counts = localGraph.second.getOrPut(prefix) { HashMap() }
                    counts[suffix] = counts.getOrDefault(suffix, 0) + 1
                }
                else {
                    val neighbors = localGraph.first.getOrPut(prefix) { HashSet() }
                    neighbors.add(suffix) // Duplicates are ignored here
                }

            }
        }
    }

    // FLUSH: Write the ENTIRE local graph to ONE file per thread
    val threadFile = File(tempFolder, "t${threadId}.ex2")
    threadFile.bufferedWriter().use { writer ->
        if (NEED_COUNT_4_Ex2) {
            for ((prefix, suffixMap) in localGraph.second) {
                val sb = StringBuilder()
                for ((suffix,count) in suffixMap) {
                    //OPTION B: For debugging or weighted graph (AA>AT:3) - Uncomment if needed
                    sb.append("$suffix:$count").append(DELIMITER)
                }
                writer.write("$prefix$CONNECTOR")
                writer.write(sb.toString())
                writer.newLine()
            }
        }
        else {
            for ((prefix, suffixMap) in localGraph.first) {
                val sb = StringBuilder()
                for (suffix in suffixMap) {
                    //OPTION A: Deduplicated format (AA>AT) - Recommended for Performance & Contigs
                    sb.append(suffix).append(DELIMITER)
                }
                writer.write("$prefix$CONNECTOR")
                writer.write(sb.toString())
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
    val tempDir = File(tempFolder)
    if (tempDir.exists()) tempDir.deleteRecursively() // Clear old temp files
    tempDir.mkdirs()

    val queue = LinkedBlockingQueue<LineBatch>(EX2_QUEUE_CAPACITY)
    val executor = Executors.newFixedThreadPool(numThreads)

    // Pass the thread index (it) as the threadId
    val futures = (0..<numThreads).map { threadId ->
        executor.submit { buildGlobalDeBruijnWorker(threadId, queue, tempFolder) }
    }
    executor.shutdown()

    println("---- Reading k-mer file: $fileInput")
    val linesRead = processFileBatches(fileInput, queue, EX2_BATCH_SIZE)
    println("------ Total lines read: $linesRead")

    repeat(numThreads) { queue.put(EX2_POISON) }
    futures.forEach { it.get() }
    println("------ Finished building partial graphs per thread.")

    // Use the newly created merge function
    if (NEED_COUNT_4_Ex2)
        mergeVertexTempFilesWithWeights(fileOutput, tempFolder)
    else
        mergeGlobalDeBruijn(fileOutput, tempFolder)
}

// -----------------------------------------------------------------------------
// Run caller point
// -----------------------------------------------------------------------------
class Ex2 {
    companion object {
        @JvmStatic
        fun run(fileInput: String, fileOutput:String) {
            val numThreads = Runtime.getRuntime().availableProcessors()
            measureAndPrintTime("Finished building De Bruijn graph") {
                buildDeBruijnGraph(
                    fileInput  = fileInput,
                    fileOutput = fileOutput,
                    numThreads = numThreads
                )
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Standalone entry point for direct execution
// -----------------------------------------------------------------------------
fun main() {
    val filePath   = "SRR494099.fastq.gz"
    val fileInput  = "results/Result_1_" + filePath.substringBefore(".fastq.gz") + ".csv"
    val fileOutput = "results/Result_2_"+ filePath.substringBefore(".fastq.gz") + ".csv"
    Ex2.run(fileInput,fileOutput)
}
