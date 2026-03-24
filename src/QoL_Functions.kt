import java.io.File
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
    println("-- Execution Time: ${"%.2f".format(elapsed)} seconds")
    println("== $message ==")
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
fun mergeTempFiles(fileOutput: String, tempFolder: String, extension: String) {
    val tempFiles = File(tempFolder)
        .listFiles { f -> f.name.endsWith(extension) }
        ?.sortedBy { f -> f.nameWithoutExtension.toIntOrNull() ?: Int.MAX_VALUE }
        ?: emptyList()

    println("---- Merging ${tempFiles.size} temporary files in batch order in $fileOutput...")

    val outputPath = File(fileOutput).also { it.parentFile?.mkdirs() }.toPath()
    FileChannel.open(outputPath, CREATE, WRITE, TRUNCATE_EXISTING).use { out ->
        for (temp in tempFiles) {
            FileChannel.open(temp.toPath(), READ).use { inp ->
                var pos = 0L
                val size = inp.size()
                while (pos < size)
                    pos += inp.transferTo(pos, size - pos, out)
            }
            try { temp.delete() } catch (e: Exception) { println(">>>> Erro ao eliminar ${temp.name}: $e") }
        }
    }
    println("-- Merge complete.")
}


/**
 * Parses one graph line from Ex2 output into a **distinct** adjacency map.
 *
 * Duplicate suffixes (e.g. `TG,TG,TG`) are deduplicated via [HashSet] because
 * contig analysis is structural: the branching/non-branching property of a node
 * depends on how many *distinct* in/out edges it has, not on edge multiplicities.
 *
 * Format: `prefix>suf1,suf2,...;prefix2>...;`
 */
fun parseGraph(line: String): HashMap<String, HashSet<String>> {
    val graph = HashMap<String, HashSet<String>>(16)

    var pos = 0
    while (pos < line.length) {
        val semiIdx = line.indexOf(SEPARATOR, pos)
        val end = if (semiIdx == -1) line.length else semiIdx
        val entry = line.substring(pos, end)
        pos = end + 1
        if (entry.isEmpty()) continue

        val arrowIdx = entry.indexOf(CONNECTOR)
        if (arrowIdx < 0) continue

        val prefix = entry.substring(0, arrowIdx)
        val suffixPart = entry.substring(arrowIdx + 1)
        if (prefix.isEmpty() || suffixPart.isEmpty()) continue

        val neighbors = graph.getOrPut(prefix) { HashSet(4) }
        var s = 0
        while (s < suffixPart.length) {
            val commaIdx = suffixPart.indexOf(DELIMITER, s)
            val eod = if (commaIdx == -1) suffixPart.length else commaIdx
            val suf = suffixPart.substring(s, eod)
            if (suf.isNotEmpty()) neighbors.add(suf)   // HashSet deduplicates
            s = eod + 1
        }
    }

    return graph
}