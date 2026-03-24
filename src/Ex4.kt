/**
 * O objetivo deste exercício é obter os contigs de um grafo de Bruijn.
 * Um contig é um caminho não ramificado máximo num grafo de Bruijn.
 * Um caminho é considerado não ramificado se in(v) = out(v) = 1 para cada nó intermediário v desse caminho,
 * ou seja, para cada nó, exceto possivelmente o nó inicial e final de um caminho.
 * Um caminho não ramificado máximo é um caminho não ramificado que não pode ser estendido para um caminho não
 * ramificado mais longo.
 */

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue



// -----------------------------------------------------------------------------
// Batch model
// -----------------------------------------------------------------------------

private val EX4_POISON = LineBatch(-1, emptyList())

private const val EX4_QUEUE_CAPACITY = 20
private const val EX4_BATCH_SIZE = 10_000


// -----------------------------------------------------------------------------
// Contig extraction
// -----------------------------------------------------------------------------

/**
 * Reconstructs the string sequence from a node path in the De Bruijn graph.
 *
 * In a k-mer De Bruijn graph each node label is a (k-1)-mer.  The sequence
 * for a path v₀→v₁→…→vₙ is: v₀ + last(v₁) + last(v₂) + … + last(vₙ)
 * (each edge adds exactly one new character — the last char of the child node).
 */
private fun pathToSequence(path: List<String>): String {
    if (path.isEmpty()) return ""
    val sb = StringBuilder(path[0])
    for (i in 1..<path.size) sb.append(path[i].last())
    return sb.toString()
}

/**
 * Finds all **maximal non-branching paths** (contigs) in [graph].
 *
 * ### Algorithm
 * 1. Compute **distinct** in/out degree for every node (edge multiplicities
 *    are irrelevant; only the existence of an edge matters).
 * 2. A node is **branching** if `distinctOut(v) ≠ 1` OR `distinctIn(v) ≠ 1`.
 *    This includes sources (in=0), sinks (out=0), and true junctions.
 * 3. For every branching node v, follow each distinct outgoing edge through
 *    consecutive non-branching nodes until reaching another branching node
 *    (or a sink). Each such path is one contig.
 * 4. Detect **isolated cycles**: non-branching nodes unreachable from any
 *    branching node form closed loops — each loop is its own contig.
 *
 * Returns a list of reconstructed sequence strings, one per contig.
 */
private fun getContigs(graph: HashMap<String, HashSet<String>>): List<String> {
    if (graph.isEmpty()) return emptyList()

    // Collect every node (sources AND pure-sink targets)
    val allNodes = HashSet<String>(graph.size * 2)
    for ((v, neighbors) in graph) {
        allNodes.add(v)
        allNodes.addAll(neighbors)
    }

    // Compute distinct in/out degrees
    val outDeg = HashMap<String, Int>(allNodes.size)
    val inDeg  = HashMap<String, Int>(allNodes.size)
    for (n in allNodes) { outDeg[n] = 0; inDeg[n] = 0 }
    for ((v, neighbors) in graph) {
        outDeg[v] = neighbors.size
        for (u in neighbors) inDeg[u] = (inDeg[u] ?: 0) + 1
    }

    fun isBranching(v: String) = (outDeg[v] ?: 0) != 1 || (inDeg[v] ?: 0) != 1

    // The single distinct successor of a non-branching node (safe to call only when outDeg=1)
    fun successor(v: String): String = graph[v]!!.first()

    val contigs = mutableListOf<String>()

    // visited: tracks non-branching nodes already assigned to a contig to
    // avoid duplicates and detect isolated cycles in one pass.
    val visited = HashSet<String>()

    // ── Phase 1: paths that START at a branching node ──────────────────────
    for (v in allNodes) {
        if (isBranching(v) && graph.containsKey(v)) {
            visited.add(v)
            for (w in graph[v]!!) {
                val path = mutableListOf(v)
                var cur = w
                // Traverse non-branching nodes until a branching one is reached
                while (!isBranching(cur) && cur !in visited) {
                    visited.add(cur)
                    path.add(cur)
                    cur = successor(cur)
                }
                path.add(cur)   // terminal node (branching or cycle entry)
                contigs.add(pathToSequence(path))
            }
        }
    }

    // ── Phase 2: isolated cycles (non-branching nodes never reached above) ──
    for (v in allNodes) {
        if (!isBranching(v) && v !in visited && graph.containsKey(v)) {
            val path = mutableListOf(v)
            visited.add(v)
            var cur = successor(v)
            while (cur != v && cur !in visited) {
                visited.add(cur)
                path.add(cur)
                cur = successor(cur)
            }
            path.add(v)  // close the cycle back to the start node
            contigs.add(pathToSequence(path))
        }
    }

    return contigs
}

// -----------------------------------------------------------------------------
// Worker — one batch → one temp file with one contig-list line per graph line
// -----------------------------------------------------------------------------

/**
 * Consumes [ContigBatch]es until the poison pill arrives.
 *
 * For **each input line** in the batch:
 *  1. Parses the distinct De Bruijn graph.
 *  2. Finds all maximal non-branching paths.
 *  3. Writes one output line: `seq1,seq2,...`
 *
 * Temp file: `{batchId}.ex4`, one contig-list line per input graph line.
 */
private fun contigWorker(
    queue: LinkedBlockingQueue<LineBatch>,
    tempFolder: String
) {
    while (true) {
        val batch = queue.take()
        if (batch.batchId == -1) break

        val tempFile = File(tempFolder, "${batch.batchId}.ex4")
        tempFile.bufferedWriter().use { writer ->
            for (line in batch.lines) {
                if (line.isBlank()) continue

                val graph = parseGraph(line)
                if (graph.isEmpty()) continue

                val contigs = getContigs(graph)
                if (contigs.isEmpty()) continue

                // One output line: contig1,contig2,...
                writer.write(contigs.joinToString(SEPARATOR.toString()))
                writer.newLine()
            }
        }
    }
}


// -----------------------------------------------------------------------------
// Main pipeline
// -----------------------------------------------------------------------------

/**
 * Finds contigs for each De Bruijn graph line in [fileInput] (Ex2 output)
 * using a producer-consumer pipeline:
 *
 * 1. [numThreads] workers start and block on queue.
 * 2. The calling thread reads [fileInput] line by line, enqueues batches.
 * 3. Each worker: per input line → parse distinct graph → find contigs → write.
 * 4. Temp files are concatenated in batch order → [fileOutput].
 *
 * Output: one line per read, format `seq1,seq2,...` (comma-separated contigs).
 */
fun findContigs(
    fileInput: String,
    fileOutput: String,
    numThreads: Int = Runtime.getRuntime().availableProcessors(),
    tempFolder: String = "temporary_files/ex4/"
) {
    File(tempFolder).mkdirs()

    val queue = LinkedBlockingQueue<LineBatch>(EX4_QUEUE_CAPACITY)
    val executor = Executors.newFixedThreadPool(numThreads)

    val futures = (0..<numThreads).map {
        executor.submit { contigWorker(queue, tempFolder) }
    }
    executor.shutdown()

    println("---- Reading De Bruijn graph file: $fileInput")
    val linesRead = processFileBatches(fileInput, queue, EX4_BATCH_SIZE)
    println("---- Total graph lines read: $linesRead")

    repeat(numThreads) { queue.put(EX4_POISON) }
    futures.forEach { it.get() }
    println("---- Finished extracting contigs.")

    mergeTempFiles(fileOutput, tempFolder, extension = ".ex4")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fun main() {
    val filePath   = "SRR494099.fastq.gz"
    val fileInput  = "results/Result_2_" + filePath.substring(0, filePath.length-9) + ".csv"
    val fileOutput = "results/Result_4_"+ filePath.substring(0, filePath.length-9) + ".csv"
    val numThreads = Runtime.getRuntime().availableProcessors()

    println("---- Using $numThreads threads (availableProcessors)")

    measureAndPrintTime("Finished extracting contigs") {
        findContigs(
            fileInput  = fileInput,
            fileOutput = fileOutput,
            numThreads = numThreads
        )
    }
}