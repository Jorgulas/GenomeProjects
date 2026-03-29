/**
 * Um percurso Euleriano num grafo é um percurso que passa por todas as arestas do grafo exatamente uma vez. Para
 * existir um grafo Caminho Euleriano tem de existir no máximo um vértice que tem (outdegree)- (indegree) = 1 e
 * no máximo um vértice tem (indegree)- (outdegree) = 1. Todos os outros vértices têm indegree e outdegree iguais.
 * Neste trabalho assuma que os dados utilizados permitem sempre a existência de um percurso Euleriano. Para obter o
 * percurso Euleriano, deverá utilizar o algoritmo de Hierholzer.
 */

import java.io.File

// -----------------------------------------------------------------------------
// Start-vertex selection (Global Graph)
// -----------------------------------------------------------------------------

private fun findStartVertex(graph: Map<String, MutableList<String>>): String {
    val outDeg = HashMap<String, Int>(graph.size * 2)
    val inDeg  = HashMap<String, Int>(graph.size * 2)

    // Calculate In-Degrees and Out-Degrees
    for ((prefix, neighbors) in graph) {
        outDeg[prefix] = (outDeg[prefix] ?: 0) + neighbors.size
        for (suf in neighbors) {
            inDeg[suf] = (inDeg[suf] ?: 0) + 1
        }
    }

    // Find the mathematical start of the Eulerian path
    for ((v, out) in outDeg) {
        if (out - (inDeg[v] ?: 0) == 1) {
            println("------ Found specific start vertex: $v")
            return v
        }
    }

    // If perfect cycles exist everywhere, just pick the first available node
    val fallback = graph.keys.firstOrNull() ?: ""
    println("------ No unbalanced start node found. Defaulting to: $fallback")
    return fallback
}

// -----------------------------------------------------------------------------
// Hierholzer's algorithm (Global Graph)
// -----------------------------------------------------------------------------

/**
 * Finds an Eulerian path in the [graph] using Hierholzer's algorithm.
 * * Note: We use a MutableList instead of HashSet here because if an edge
 * appeared 5 times in the original reads, we must walk that edge 5 times!
 */
private fun hierholzerGlobal(graph: HashMap<String, MutableList<String>>): ArrayDeque<String> {
    val start = findStartVertex(graph)
    if (start.isEmpty()) return ArrayDeque()

    val stack = ArrayDeque<String>()
    val path  = ArrayDeque<String>()

    stack.addLast(start)

    while (stack.isNotEmpty()) {
        val v = stack.last()
        val neighbors = graph[v]

        if (!neighbors.isNullOrEmpty()) {
            // Remove the last edge to mark it as "visited" (O(1) operation)
            val next = neighbors.removeLast()
            stack.addLast(next)
        } else {
            // Dead end reached, push to final path and backtrack
            path.addFirst(stack.removeLast())
        }
    }
    return path
}

// -----------------------------------------------------------------------------
// Main sequential pipeline
// -----------------------------------------------------------------------------

/**
 * Sequentially loads the global graph, reconstructing edge frequencies from
 * the weighted format, and calculates the Eulerian Path.
 */
fun findEulerianPaths(fileInput: String, fileOutput: String) {
    println("---- Loading entire Global Graph into memory for Eulerian Path...")

    // Using MutableList instead of HashSet to allow duplicate edges (weights)
    val globalGraph = HashMap<String, MutableList<String>>()
    var totalEdges = 0

    File(fileInput).useLines { lines ->
        lines.forEach { line ->
            if (line.isNotBlank()) {
                val arrowIdx = line.indexOf(CONNECTOR)
                val semiIdx = line.indexOf(SEPARATOR)

                if (arrowIdx != -1) {
                    val prefix = line.substring(0, arrowIdx)

                    // Extract neighbors and counts
                    val suffixStr = if (semiIdx != -1) line.substring(arrowIdx + 1, semiIdx)
                    else line.substring(arrowIdx + 1)
                    val countStr = if (semiIdx != -1 && semiIdx + 1 < line.length) line.substring(semiIdx + 1)
                    else ""

                    val neighbors = suffixStr.split(DELIMITER).filter { it.isNotBlank() }
                    val counts = countStr.split(DELIMITER).filter { it.isNotBlank() }.mapNotNull { it.toIntOrNull() }

                    val edgeList = globalGraph.getOrPut(prefix) { mutableListOf() }

                    // If we have weights, expand them! (e.g., AC: 3 -> add AC, AC, AC)
                    if (counts.size == neighbors.size) {
                        for (i in neighbors.indices) {
                            repeat(counts[i]) {
                                edgeList.add(neighbors[i])
                                totalEdges++
                            }
                        }
                    } else {
                        // Fallback if weights are missing
                        edgeList.addAll(neighbors)
                        totalEdges += neighbors.size
                    }
                }
            }
        }
    }

    println("------ Graph loaded. Nodes: ${globalGraph.size} | Total Edges: $totalEdges")
    println("---- Calculating Eulerian path...")

    val path = hierholzerGlobal(globalGraph)

    println("------ Path calculated. Path length: ${path.size} nodes.")
    println("---- Writing output to $fileOutput...")

    File(fileOutput).also { it.parentFile?.mkdirs() }.bufferedWriter().use { writer ->
        if (path.isNotEmpty()) {
            writer.write(path.joinToString("->"))
            writer.newLine()
        } else {
            writer.write("No path found.")
        }
    }
    println("------ Done.")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------
class Ex3 {
    companion object {
        @JvmStatic
        fun run(fileInput: String, fileOutput: String) {
            measureAndPrintTime("Finished finding global Eulerian path") {
                findEulerianPaths(
                    fileInput  = fileInput,
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
    val filePath   = "SRR494099.fastq.gz"
    val fileInput  = "results/Result_2_" + filePath.substring(0, filePath.length-9) + ".csv"
    val fileOutput = "results/Result_3_" + fileInput.substring(0, fileInput.length-9) + ".csv"
    Ex3.run(fileInput, fileOutput)
}