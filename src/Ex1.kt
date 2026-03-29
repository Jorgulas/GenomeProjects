
/***
## Obter os k-mers a partir dos reads

O objetivo deste exercício é, dado um conjunto de reads e dado um valor k, obter os k-mers
a partir desses reads. Por exemplo, considerando o read ATCGATCAC e k=3, os k-mers são:
ATC, TCG, CGA, GAT, ATC, TCA, CAC
Este exercício deverá suportar como ‘input’ um FASTAQ file, para poder ser testado com datasets
reais tais como os que se podem obter a partir do ENA https://www.ebi.ac.uk/ena/browser/,
onde poderão ser obtidos ficheiros de reads de determinada amostra biológica, como, por
exemplo, o SRR494099.

O ‘output’ deste exercício deverá poder ser utilizado como ‘input’ para o exercício 2.
 **/

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream

// -----------------------------------------------------------------------------
// Tuning constants
// -----------------------------------------------------------------------------

/** Queues batches of lines to limit memory while preserving order. */
private const val QUEUE_CAPACITY = 20
private const val BATCH_SIZE = 100_000

private val POISON_BATCH = LineBatch(-1, emptyList())

/** GZIPInputStream internal read buffer (8 MB) – fewer decompression round-trips. */
private const val GZIP_BUFFER = 8 * 1024 * 1024

/** Pre-built spaces for high-speed padding. */
private const val MIN_LINE_WIDTH = 135
private val SPACES = " ".repeat(MIN_LINE_WIDTH)

// -----------------------------------------------------------------------------
// FASTQ filtering — no Regex overhead, plain char comparisons
// -----------------------------------------------------------------------------

/** True for FASTQ metadata/quality-score header lines and blank lines. */
@Suppress("NOTHING_TO_INLINE")
private inline fun isMetadataLine(line: String) =
    line.isEmpty() || !isValidChar(line[0])

/** True iff [c] is a valid nucleotide symbol (A, T, C, G or N). */
@Suppress("NOTHING_TO_INLINE")
private inline fun isValidChar(c: Char) =
    c == 'A' || c == 'T' || c == 'C' || c == 'G' || c == 'N'

/** Strips all characters that are not A, T, C, G, or N. */
private fun filterInvalid(line: String): String = line.filter(::isValidChar)

/**
 * Filter invalid characters from a line.
 * Fast-path: if the line is already clean, returns the original sequence.
 */
private fun toCleanSequence(line: String): String? {
    var allValid = true
    for (i in 0..<line.length) {
        if (!isValidChar(line[i])) {
            allValid = false
            break
        }
    }
    if (allValid) return line.ifEmpty { null }
    val filtered = filterInvalid(line)
    return filtered.ifEmpty { null }
}


// -----------------------------------------------------------------------------
// Worker — drains the queue into a temp file
// -----------------------------------------------------------------------------

/**
 * Consumes sequence lines from [queue] until a `null` poison-pill arrives,
 * computes k-mers, and writes results to `<batchId>.ex1` inside [tempFolder].
 *
 * **Optimizations:**
 * - K-mer CSV is assembled **directly in [sb]** with index-based iteration —
 *   no intermediate String allocation (no `joinToString`).
 * - [sb] is only flushed to disk when it exceeds [BATCH_SIZE_BYTES], minimizing
 *   the number of OS write syscalls.
 */
private fun workerTask(
    queue: LinkedBlockingQueue<LineBatch>,
    k: Int,
    tempFolder: String
) {

    while (true) {
        val batch = queue.take()
        if (batch === POISON_BATCH || batch.batchId == -1) break

        val tempFile = File(tempFolder, "${batch.batchId}.ex1")
        tempFile.bufferedWriter().use { writer ->
            for (rawLine in batch.lines) {
                // FIX: Remove hidden \r or trailing spaces that mess up k-mer windows
                val line = rawLine.trim()
                if (line.isEmpty()) continue

                var lineLengthWritten = 0
                var firstKmerInLine = true

                // First pass to check if we should skip (matches kmers.none())
                if (!hasAnyKmers(line, k)) continue

                val len = line.length
                var spanStart = 0
                for (i in 0..len) {
                    // Split on 'N' or 'n'
                    if (i == len || line[i].equals('N', ignoreCase = true)) {
                        for (j in spanStart..(i - k)) {
                            if (!firstKmerInLine) {
                                writer.write(SEPARATOR.toString())
                                lineLengthWritten++
                            }
                            writer.write(line, j, k)
                            lineLengthWritten += k
                            firstKmerInLine = false
                        }
                        spanStart = i + 1
                    }
                }

                // Padding: Matches sb.append(SPACES, 0, 135 - wordLen)
                val paddingNeeded = MIN_LINE_WIDTH - lineLengthWritten
                if (paddingNeeded > 0) {
                    writer.write(SPACES, 0, paddingNeeded)
                }
                writer.newLine()
            }
        }
    }
}

/**
 * Quick check to see if a line contains at least one valid k-mer span.
 * This replaces 'kmers.none()' without allocating any strings.
 */
private fun hasAnyKmers(line: String, k: Int): Boolean {
    var spanStart = 0
    val len = line.length
    for (i in 0..len) {
        if (i == len || line[i].equals('N', ignoreCase = true)) {
            if (i - spanStart >= k) return true
            spanStart = i + 1
        }
    }
    return false
}


// -----------------------------------------------------------------------------
// Main pipeline — streaming producer-consumer
// -----------------------------------------------------------------------------

/**
 * Processes a gzipped FASTQ file with a **streaming producer-consumer** pattern:
 *
 * 1. [numThreads] worker threads start and block on queue.
 * 2. The calling thread acts as the **single reader**: streams the `.gz` file with
 *    an 8 MB decompression buffer, filters metadata lines, strips invalid chars,
 *    and enqueues batches of reading sequences.
 *    The [LinkedBlockingQueue] cap limits peak RAM to ~[QUEUE_CAPACITY] batches
 *    (back-pressure stalls the reader when workers are saturated).
 * 3. After EOF, one poison pill batch per worker signals end-of-stream.
 * 4. Worker temp files are merged into [fileOutput] with zero-copy NIO sequentially.
 *
 * **Memory**: the entire file is never loaded into RAM — only [QUEUE_CAPACITY]
 * batches are live at any moment, regardless of file size.
 */
fun getKMersParallel(
    filePath: String,
    k: Int,
    numThreads: Int = Runtime.getRuntime().availableProcessors(),
    fileOutput: String,
    tempFolder: String = "temporary_files/ex1/"
) {
    require(k >= 3) { "`k` deve ser >= 3" }
    File(tempFolder).mkdirs()

    val queue = LinkedBlockingQueue<LineBatch>(QUEUE_CAPACITY)
    val executor = Executors.newFixedThreadPool(numThreads)

    // Start workers first so they are ready to consume immediately
    val futures = (0..<numThreads).map {
        executor.submit { workerTask(queue, k, tempFolder) }
    }
    executor.shutdown()   // no new tasks; existing ones keep running

    // ---- Reader (calling thread) ----
    println("---- Reading FASTQ file: $filePath")
    var linesEnqueued = 0L
    var batchId = 0
    var currentBatch = ArrayList<String>(BATCH_SIZE)

    BufferedReader(
        InputStreamReader(GZIPInputStream(File(filePath).inputStream(), GZIP_BUFFER))
    ).use { reader ->
        reader.forEachLine { raw ->
            val line = raw.trim()
            if (!isMetadataLine(line)) {
                val clean = toCleanSequence(line)
                if (clean != null) {
                    currentBatch.add(clean)
                    linesEnqueued++
                    if (currentBatch.size >= BATCH_SIZE) {
                        queue.put(LineBatch(batchId++, currentBatch))
                        currentBatch = ArrayList(BATCH_SIZE)
                    }
                }
            }
        }
        if (currentBatch.isNotEmpty()) {
            queue.put(LineBatch(batchId++, currentBatch))
        }
    }
    println("------ Total sequence lines enqueued: $linesEnqueued")

    // One poison pill per worker
    repeat(numThreads) { queue.put(POISON_BATCH) }

    // Await completion
    futures.forEach { it.get() }
    println("------ Finished processing k-mers in parallel.")
    mergeTempFiles(fileOutput, tempFolder, extension = ".ex1")
}


// -----------------------------------------------------------------------------
// Run caller point
// -----------------------------------------------------------------------------
class Ex1 {
    companion object {
        @JvmStatic
        fun run(fileInput: String, fileOutput:String, k: Int) {
            val numThreads = Runtime.getRuntime().availableProcessors()
            measureAndPrintTime("Finished analyzing file $fileInput") {
                getKMersParallel(
                    filePath   = fileInput,
                    k          = k,
                    numThreads = numThreads,
                    fileOutput = fileOutput
                )
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Standalone entry point for direct execution
// -----------------------------------------------------------------------------
fun main() {
    val fileInput   = "SRR20964298_1.fastq.gz"
    val fileOutput = "results/Result_1_" + fileInput.substring(0, fileInput.length-9) + ".csv"
    val k          = 6

    println("---- First 10 lines of $fileInput:")
    BufferedReader(
        InputStreamReader(GZIPInputStream(File(fileInput).inputStream(), GZIP_BUFFER))
    ).use { reader ->
        var i = 0
        while (i  < 10) {
            val line = reader.readLine()
            if (isMetadataLine(line)) continue
            println("\t"+line)
            i++
        }
    }
    Ex1.run(fileInput,fileOutput, k)
}