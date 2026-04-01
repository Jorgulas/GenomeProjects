/**
 * Obter os k-mers a partir dos reads
 */

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPInputStream
import kotlin.math.roundToInt

// -----------------------------------------------------------------------------
// Tuning constants
// -----------------------------------------------------------------------------

private const val QUEUE_CAPACITY = 20
private const val BATCH_SIZE = 200_000

private val POISON_BATCH = LineBatch(-1, emptyList())
private const val GZIP_BUFFER = 32 * 1024 * 1024
private const val abuseFactor4Threads = 1.5 // 1.5x threads to better saturate CPU during I/O waits

private const val MIN_LINE_WIDTH = 135
private val SPACES = " ".repeat(MIN_LINE_WIDTH)

// -----------------------------------------------------------------------------
// FASTQ filtering
// -----------------------------------------------------------------------------

@Suppress("NOTHING_TO_INLINE")
private inline fun isMetadataLine(line: String) =
    line.isEmpty() || !isValidChar(line[0])

@Suppress("NOTHING_TO_INLINE")
private inline fun isValidChar(c: Char) =
    c == 'A' || c == 'T' || c == 'C' || c == 'G' || c == 'N'

private fun filterInvalid(line: String): String {
    val sb = java.lang.StringBuilder(line.length)
    for (i in 0..<line.length) {
        val c = line[i]
        if (isValidChar(c)) sb.append(c)
    }
    return sb.toString()
}

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
// Worker — Hyper-Optimized Inner Loop
// -----------------------------------------------------------------------------

private fun workerTask(
    queue: LinkedBlockingQueue<LineBatch>,
    k: Int,
    tempFolder: String
) {
    // Cache primitive codes to completely avoid String allocations in the loop
    val sepCode = SEPARATOR.code
    val newLineCode = '\n'.code

    while (true) {
        val batch = queue.take()
        if (batch === POISON_BATCH || batch.batchId == -1) break

        val tempFile = File(tempFolder, "${batch.batchId}.ex1")
        tempFile.bufferedWriter().use { writer ->

            for (line in batch.lines) {
                val len = line.length
                if (len == 0) continue

                var lineLengthWritten = 0
                var firstKmerInLine = true
                var spanStart = 0

                // Single pass over the characters
                for (i in 0..len) {
                    // Force an 'N' at the end of the line to trigger the final span processing
                    val c = if (i < len) line[i] else 'N'

                    // Primitive comparison is much faster than .equals(ignoreCase = true)
                    if (c == 'N' || c == 'n') {
                        val spanLen = i - spanStart

                        // Only process if the valid chunk is large enough for at least one k-mer
                        if (spanLen >= k)
                            for (j in spanStart..(i - k)) {
                                if (!firstKmerInLine) {
                                    writer.write(sepCode) // Primitive write, zero allocation
                                    lineLengthWritten++
                                }
                                writer.write(line, j, k) // Substring write without creating a String
                                lineLengthWritten += k
                                firstKmerInLine = false
                            }

                        spanStart = i + 1 // Move start past the 'N'
                    }
                }

                // Only write padding and newline if we actually outputted k-mers
                if (lineLengthWritten > 0) {
                    val paddingNeeded = MIN_LINE_WIDTH - lineLengthWritten
                    if (paddingNeeded > 0)
                        writer.write(SPACES, 0, paddingNeeded)

                    writer.write(newLineCode)
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Main pipeline
// -----------------------------------------------------------------------------

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

    val futures = (0..<numThreads).map {
        executor.submit { workerTask(queue, k, tempFolder) }
    }
    executor.shutdown()

    println("---- Reading FASTQ file: $filePath with $numThreads threads for k-mer extraction...")
    var linesEnqueued = 0L
    var batchId = 0
    var currentBatch = ArrayList<String>(BATCH_SIZE)

    BufferedReader(
        InputStreamReader(GZIPInputStream(File(filePath).inputStream(), GZIP_BUFFER))
    ).use { reader ->
        // reader.readLine() is slightly faster than forEachLine because it avoids lambda instantiation
        var line = reader.readLine()
        while (line != null) {
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
            line = reader.readLine()
        }
        if (currentBatch.isNotEmpty())
            queue.put(LineBatch(batchId++, currentBatch))
    }
    println("------ Total sequence lines enqueued: $linesEnqueued")

    repeat(numThreads) { queue.put(POISON_BATCH) }
    futures.forEach { it.get() }

    println("------ Finished processing k-mers in parallel.")
    mergeTempFilesParallel(fileOutput, tempFolder, extension = ".ex1")
}

// -----------------------------------------------------------------------------
// Run caller point
// -----------------------------------------------------------------------------
class Ex1 {
    companion object {
        @JvmStatic
        fun run(fileInput: String, fileOutput:String, k: Int) {
            val numThreads = (Runtime.getRuntime().availableProcessors() * abuseFactor4Threads).roundToInt()
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
// Standalone entry point
// -----------------------------------------------------------------------------
fun main() {
    val fileInput   = "SRR494099.fastq.gz"
    val fileOutput = "results/Result_1_" + fileInput.substringBefore(".fastq.gz") + ".csv"
    val k          = 8

    println("---- First 5 lines of $fileInput:")
    BufferedReader(
        InputStreamReader(GZIPInputStream(File(fileInput).inputStream(), GZIP_BUFFER))
    ).use { reader ->
        var i = 0
        while (i < 5) {
            val line = reader.readLine() ?: break
            if (isMetadataLine(line)) continue
            println("\t$line")
            i++
        }
    }
    Ex1.run(fileInput, fileOutput, k)
}