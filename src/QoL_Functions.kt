import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE

const val CONNECTOR = '>'   // prefix > suffixes
const val DELIMITER = ','   // suffix,suffix
const val SEPARATOR = ';'   // entry;entry

class LineBatch(val batchId: Int, val lines: List<String>)

inline fun measureAndPrintTime(message: String, block: () -> Unit) {
    val startTime = System.currentTimeMillis()
    block()
    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    println("»» Execution Time: ${"%.2f".format(elapsed)} seconds")
    println("»»» $message «««")
}

fun processFileBatches(
    fileInput: String,
    queue: java.util.concurrent.LinkedBlockingQueue<LineBatch>,
    batchSize: Int
): Long {
    var linesRead = 0L
    var batchId = 0
    var currentBatch = ArrayList<String>(batchSize)

    File(fileInput).bufferedReader().use { reader ->
        reader.forEachLine { rawLine ->
            if (rawLine.isNotBlank()) {
                currentBatch.add(rawLine)
                linesRead++
                if (currentBatch.size >= batchSize) {
                    queue.put(LineBatch(batchId++, currentBatch))
                    currentBatch = ArrayList(batchSize)
                }
            }
        }
        if (currentBatch.isNotEmpty()) {
            queue.put(LineBatch(batchId++, currentBatch))
        }
    }
    return linesRead
}

/**
 * Merges all `*[extension]` files in [tempFolder] (sorted numerically by stem)
 * into [fileOutput], then deletes each temp file.
 *
 * [FileChannel.transferTo] delegates to the OS-level `sendfile` (Linux) or
 * `TransmitFile` (Windows), avoiding unnecessary user-space buffer copies.
 * /**
 *  * Concatenates all `*.ex2` temp files (sorted by batchId) into [fileOutput]
 *  * using zero-copy NIO [FileChannel.transferTo], then deletes each temp file.
 *  *
 *  * Because each temp file already has one graph line per input line in the correct
 *  * order, no graph-level merging is needed — this is a plain file concatenation.
 *  */
 */
fun mergeTempFilesParallel(fileOutput: String, tempFolder: String, extension: String) {
    val tempFiles = File(tempFolder)
        .listFiles { f -> f.name.endsWith(extension) }
        ?.sortedBy { f -> f.nameWithoutExtension.toIntOrNull() ?: Int.MAX_VALUE }
        ?: emptyList()

    if (tempFiles.isEmpty()) return
    println("------ Merging ${tempFiles.size} temporary files into $fileOutput...")

    val outputPath = File(fileOutput).also { it.parentFile?.mkdirs() }.toPath()

    // 1. Calculate the exact byte offset where each file should start writing
    val fileOffsets = ArrayList<Pair<File, Long>>(tempFiles.size)
    var currentOffset = 0L
    for (file in tempFiles) {
        val size = file.length()
        fileOffsets.add(Pair(file, currentOffset))
        currentOffset += size
    }

    FileChannel.open(outputPath, CREATE, WRITE, TRUNCATE_EXISTING).use { outChannel ->
        // 2. Pre-allocate the massive file instantly on the OS level
        if (currentOffset > 0) {
            outChannel.position(currentOffset - 1)
            outChannel.write(ByteBuffer.wrap(byteArrayOf(0)))
        }

        // 3. Blast the files into the pre-allocated space concurrently
        fileOffsets.parallelStream().forEach { (file, startOffset) ->
            FileChannel.open(file.toPath(), READ).use { inChannel ->
                var pos = 0L
                val size = inChannel.size()
                while (pos < size) {
                    // transferFrom is thread-safe for specific offsets!
                    pos += outChannel.transferFrom(inChannel, startOffset + pos, size - pos)
                }
            }
            try { file.delete() } catch (_: Exception) { /* Ignore OS locks */ }
        }
    }
    println("------ merge complete.")
}


/**
 * Merges thread-specific vertex files and performs GLOBAL deduplication.
 * This ensures that even if multiple threads found the same adjacency,
 * it only appears once in the final Result_2 file.
 */
fun mergeVertexTempFilesWithWeights(fileOutput: String, tempFolder: String) {
    val tempFiles = File(tempFolder).listFiles { f -> f.name.endsWith(".ex2") } ?: return
    println("---- Merging ${tempFiles.size} thread dictionaries into global graph...")

    // Massive global map to sum everything
    val globalGraph = java.util.concurrent.ConcurrentHashMap<String, HashMap<String, Int>>()

    // We can read the few thread files in parallel
    tempFiles.toList().parallelStream().forEach { file ->
        file.useLines { lines ->
            lines.forEach { line ->
                if (line.isBlank()) return@forEach
                val arrowIdx = line.indexOf(CONNECTOR)
                val prefix = line.substring(0, arrowIdx)
                val suffixData = line.substring(arrowIdx + 1).split(DELIMITER)

                val syncMap = globalGraph.computeIfAbsent(prefix) { HashMap() }

                synchronized(syncMap) {
                    for (pair in suffixData) {
                        if (pair.contains(':')) {
                            val (suf, count) = pair.split(':')
                            syncMap[suf] = syncMap.getOrDefault(suf, 0) + count.toInt()
                        }
                    }
                }
            }
        }
        file.delete()
    }

    // Write final output
    File(fileOutput).bufferedWriter().use { writer ->
        for ((prefix, suffixMap) in globalGraph) {
            val neighbors = suffixMap.keys.toList()
            val counts = neighbors.map { suffixMap[it] }
            writer.write("$prefix$CONNECTOR${neighbors.joinToString(DELIMITER.toString())}$SEPARATOR${counts.joinToString(DELIMITER.toString())}")
            writer.newLine()
        }
    }
    println("------ Global graph generation complete.")
}

/**
 * Merges thread-specific files into a global De Bruijn graph (no weights).
 * Aggregates all suffixes for each prefix found across all threads.
 */
fun mergeGlobalDeBruijn(fileOutput: String, tempFolder: String) {
    val tempFiles = File(tempFolder).listFiles { f -> f.name.endsWith(".ex2") } ?: return

    // Global map to consolidate suffixes from all threads
    // Prefix -> Set of unique suffixes
    val globalGraph = HashMap<String, HashSet<String>>()

    println("---- Merging ${tempFiles.size} thread files into global graph...")

    for (file in tempFiles) {
        file.useLines { lines ->
            lines.forEach { line ->
                if (line.isBlank()) return@forEach

                val arrowIdx = line.indexOf(CONNECTOR)
                if (arrowIdx != -1) {
                    val prefix = line.substring(0, arrowIdx)
                    val suffixes = line.substring(arrowIdx + 1).split(DELIMITER)

                    val neighborSet = globalGraph.getOrPut(prefix) { HashSet() }
                    for (suf in suffixes) {
                        if (suf.isNotBlank()) neighborSet.add(suf)
                    }
                }
            }
        }
        file.delete() // Clean up temp file after processing
    }

    // Write final Consolidated Graph
    File(fileOutput).bufferedWriter().use { writer ->
        for ((prefix, neighbors) in globalGraph) {
            // Format: AA>AC,AT;
            writer.write("$prefix$CONNECTOR${neighbors.joinToString(DELIMITER.toString())}$SEPARATOR")
            writer.newLine()
        }
    }
    println("------ Global graph generation complete.")
}


// EXECUTE BLOCK X TIMES AND CALCULATE AVERAGE TIME
inline fun measureAverageTime(times: Int, message: String, block: () -> Unit) {
    var totalTime = 0L
    repeat(times) {
        val startTime = System.currentTimeMillis()
        block()
        totalTime += (System.currentTimeMillis() - startTime)
    }
    val average = totalTime / times.toDouble() / 1000.0
    println("»» Average Execution Time over $times runs: ${"%.2f".format(average)} seconds")
    println("»»» $message «««")
}
