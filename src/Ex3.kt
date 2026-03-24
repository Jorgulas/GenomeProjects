/**
 * Um percurso Euleriano num grafo é um percurso que passa por todas as arestas do grafo exatamente uma vez. Para
 * existir um grafo Caminho Euleriano tem de existir no máximo um vértice que tem (outdegree)- (indegree) = 1 e
 * no máximo um vértice tem (indegree)- (outdegree) = 1. Todos os outros vértices têm indegree e outdegree iguais.
 * Neste trabalho assuma que os dados utilizados permitem sempre a existˆencia de um percurso Euleriano. Para obter o
 * percurso Euleriano, deverá utilizar o algoritmo de Hierholzer. Note que pode existir mais do que um percurso. Para
 * o exemplo do exercício 1, um percurso possível será:
 * EurelianPath:
 * AA-> AT-> TG-> GG-> GG-> GA-> AT-> TG-> GC-> CC-> CA-> AT-> TG-> GT-> TT-> TA-> AA
 *
 */

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue


// -----------------------------------------------------------------------------
// Batch model
// -----------------------------------------------------------------------------

private val EX3_POISON = LineBatch(-1, emptyList())
private const val EX3_BATCH_SIZE = 10_000
private const val EX3_QUEUE_CAPACITY = 20


// -----------------------------------------------------------------------------
// Start-vertex selection (per-line graph)
// -----------------------------------------------------------------------------

private fun findStartVertex(graph: Map<String, HashSet<String>>): String {
    val outDeg = HashMap<String, Int>(graph.size * 2)
    val inDeg  = HashMap<String, Int>(graph.size * 2)

    for ((prefix, neighbors) in graph) {
        outDeg[prefix] = (outDeg[prefix] ?: 0) + neighbors.size
        for (suf in neighbors) {
            inDeg[suf] = (inDeg[suf] ?: 0) + 1
        }
    }

    for ((v, out) in outDeg) {
        if (out - (inDeg[v] ?: 0) == 1) return v
    }

    return graph.keys.first()
}

// -----------------------------------------------------------------------------
// Hierholzer's algorithm (per-line)
// -----------------------------------------------------------------------------

/**
 * Finds an Eulerian path in [graph] for a single read using Hierholzer's algorithm.
 *
 * Edges are removed via [ArrayDeque.removeFirst] as they are traversed (O(1)).
 * The graph is tiny for a single read so this is both correct and fast.
 */
private fun hierholzerLine(graph: HashMap<String, HashSet<String>>): ArrayDeque<String> {
    val start = findStartVertex(graph)
    val stack = ArrayDeque<String>()
    val path  = ArrayDeque<String>()

    stack.addLast(start)

    while (stack.isNotEmpty()) {
        val v = stack.last()
        val neighbors = graph[v]
        if (!neighbors.isNullOrEmpty()) {
            val first = neighbors.first()
            stack.addLast(first)
            neighbors.remove(first)
            continue
        }
        path.addFirst(stack.removeLast())
    }
    return path
}

// -----------------------------------------------------------------------------
// Worker — processes one batch → one temp file with one path line per graph line
// -----------------------------------------------------------------------------

/**
 * Consumes [GraphLineBatch]es from [queue] until the poison pill arrives.
 *
 * For **each line** in the batch:
 *  1. Parses the De Bruijn graph for that read.
 *  2. Runs Hierholzer's algorithm to find the Eulerian path.
 *  3. Writes one output line: `v1->v2->v3->...`
 *
 * Output temp file: `{batchId}.ex3` — one path line per input graph line.
 */
private fun eulerianWorker(
    queue: LinkedBlockingQueue<LineBatch>,
    tempFolder: String
) {
    while (true) {
        val batch = queue.take()
        if (batch.batchId == -1) break

        val tempFile = File(tempFolder, "${batch.batchId}.ex3")
        tempFile.bufferedWriter().use { writer ->
            for (line in batch.lines) {
                if (line.isBlank()) continue

                val graph = parseGraph(line)
                if (graph.isEmpty()) continue

                val path = hierholzerLine(graph)
                if (path.isEmpty()) continue

                // Write path: v1->v2->v3->...
                for (i in path.indices) {
                    if (i > 0) writer.write("->")
                    writer.write(path[i])
                }
                writer.newLine()
            }
        }
    }
}


// -----------------------------------------------------------------------------
// Main pipeline
// -----------------------------------------------------------------------------

/**
 * Finds an Eulerian path for each De Bruijn graph line in [fileInput] (Ex2 output)
 * using a producer-consumer pipeline:
 *
 * 1. [numThreads] workers start and block on queue.
 * 2. The calling thread reads [fileInput] line by line, enqueues batches.
 * 3. Each worker: per input line → parse graph → Hierholzer → one output line.
 * 4. Temp files are concatenated in batch order → [fileOutput].
 *
 * Output: one Eulerian path per line, format `v1->v2->v3->...`
 */
fun findEulerianPaths(
    fileInput: String,
    fileOutput: String,
    numThreads: Int = Runtime.getRuntime().availableProcessors(),
    tempFolder: String = "temporary_files/ex3/"
) {
    File(tempFolder).mkdirs()

    val queue = LinkedBlockingQueue<LineBatch>(EX3_QUEUE_CAPACITY)
    val executor = Executors.newFixedThreadPool(numThreads)

    val futures = (0..<numThreads).map {
        executor.submit { eulerianWorker(queue, tempFolder) }
    }
    executor.shutdown()

    println("---- Reading graph file: $fileInput")
    val linesRead = processFileBatches(fileInput, queue, EX3_BATCH_SIZE)
    println("---- Total graph lines read: $linesRead")

    // One poison pill per worker to signal end-of-stream
    repeat(numThreads) { queue.put(EX3_POISON) }
    futures.forEach { it.get() }
    println("---- Finished computing Eulerian paths.")

    mergeTempFiles(fileOutput, tempFolder, extension = ".ex3")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------

fun main() {
    val fileInput  = "results/Result_2.csv"
    val fileOutput = "results/Result_3.csv"
    val numThreads = Runtime.getRuntime().availableProcessors()

    println("---- Using $numThreads threads (availableProcessors)")

    measureAndPrintTime("Finished finding Eulerian paths") {
        findEulerianPaths(
            fileInput  = fileInput,
            fileOutput = fileOutput,
            numThreads = numThreads
        )
    }
}