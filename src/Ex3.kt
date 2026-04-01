/**
 * Um percurso Euleriano num grafo é um percurso que passa por todas as arestas do grafo exatamente uma vez.
 * Neste trabalho assuma que os dados utilizados permitem sempre a existência de um percurso Euleriano.
 * Para obter o percurso Euleriano, deverá utilizar o algoritmo de Hierholzer.
 */

import java.io.File
import kotlin.system.measureTimeMillis

// -----------------------------------------------------------------------------
// Primitive Data Structures (Zero Object Overhead)
// -----------------------------------------------------------------------------

/**
 * A memory-efficient stack using a primitive IntArray.
 * Bypasses the massive memory overhead of ArrayDeque<Int> or ArrayList<Int>.
 */
class IntStack(initialCapacity: Int = 100_000) {
    var data = IntArray(initialCapacity)
    var size = 0

    fun push(v: Int) {
        if (size == data.size) {
            data = data.copyOf(size * 2)
        }
        data[size++] = v
    }

    fun pop(): Int = data[--size]
    fun peek(): Int = data[size - 1]
    fun isNotEmpty() = size > 0
}

/**
 * Stores edges for a specific node using primitive arrays.
 */
class NodeEdges {
    var targets = IntArray(2)
    var counts = IntArray(2)
    var size = 0

    fun add(target: Int, count: Int) {
        if (size == targets.size) {
            targets = targets.copyOf(size * 2)
            counts  = counts.copyOf(size * 2)
        }
        targets[size] = target
        counts[size] = count
        size++
    }

    // Fetches the next available edge and decrements its count
    fun getNextAndDecrement(): Int {
        while (size > 0) {
            val idx = size - 1
            if (counts[idx] > 0) {
                counts[idx]--
                val target = targets[idx]
                if (counts[idx] == 0) {
                    size-- // Edge fully exhausted, remove it
                }
                return target
            } else {
                size--
            }
        }
        return -1 // No edges left
    }
}

// -----------------------------------------------------------------------------
// Global Graph State & Dictionary
// -----------------------------------------------------------------------------


// Maps a String to a unique Int ID
val idMap = HashMap<String, Int>()
// Maps the Int ID back to the String for the final output
val reverseMap = ArrayList<String>()
// The Graph, indexed by Int ID
val globalGraph = ArrayList<NodeEdges>()

/**
 * Gets or creates a unique integer ID for a string node.
 */
fun getNodeId(sequence: String): Int {
    var id = idMap[sequence]
    if (id == null) {
        id = reverseMap.size
        idMap[sequence] = id
        reverseMap.add(sequence)
        globalGraph.add(NodeEdges())
    }
    return id
}

// -----------------------------------------------------------------------------
// Start-vertex selection (Global Graph)
// -----------------------------------------------------------------------------

private fun findStartVertex(): Int {
    val nodeCount = globalGraph.size
    val outDeg = IntArray(nodeCount)
    val inDeg  = IntArray(nodeCount)

    // Calculate In-Degrees and Out-Degrees
    for (i in 0 until nodeCount) {
        val edges = globalGraph[i]
        for (j in 0 until edges.size) {
            val target = edges.targets[j]
            val count = edges.counts[j]
            outDeg[i] += count
            inDeg[target] += count
        }
    }

    // Find the mathematical start of the Eulerian path
    for (i in 0 until nodeCount) {
        if (outDeg[i] - inDeg[i] == 1) {
            println("------ Found specific start vertex: ${reverseMap[i]}")
            return i
        }
    }

    println("------ No unbalanced start node found. Defaulting to first node.")
    return 0
}

// -----------------------------------------------------------------------------
// Hierholzer's algorithm (Primitive Global Graph)
// -----------------------------------------------------------------------------

private fun hierholzerGlobal(): IntStack {
    if (globalGraph.isEmpty()) return IntStack(0)

    val start = findStartVertex()
    val stack = IntStack()
    val path  = IntStack(1_000_000) // Pre-allocate larger space for path

    stack.push(start)

    while (stack.isNotEmpty()) {
        val v = stack.peek()
        val nextNode = globalGraph[v].getNextAndDecrement()

        if (nextNode != -1) {
            stack.push(nextNode)
        } else {
            // Dead end reached, push to final path and backtrack
            path.push(stack.pop())
        }
    }
    return path
}

// -----------------------------------------------------------------------------
// Main sequential pipeline
// -----------------------------------------------------------------------------

fun findEulerianPaths(fileInput: String, fileOutput: String) {
    println("---- Loading entire Global Graph into memory using Dictionary Encoding...")

    // Clear globals in case of multiple runs
    idMap.clear()
    reverseMap.clear()
    globalGraph.clear()
    var totalEdges = 0

    File(fileInput).useLines { lines ->
        lines.forEach { line ->
            if (line.isNotBlank()) {
                val arrowIdx = line.indexOf(CONNECTOR)

                if (arrowIdx != -1) {
                    val semiIdx = line.indexOf(SEPARATOR)

                    val prefixStr = line.substring(0, arrowIdx)
                    val prefixId = getNodeId(prefixStr)

                    val suffixStr = if (semiIdx != -1) line.substring(arrowIdx + 1, semiIdx) else line.substring(arrowIdx + 1)
                    val countStr = if (semiIdx != -1 && semiIdx + 1 < line.length) line.substring(semiIdx + 1) else ""

                    val neighbors = suffixStr.split(DELIMITER).filter { it.isNotBlank() }
                    val counts = countStr.split(DELIMITER).filter { it.isNotBlank() }.mapNotNull { it.toIntOrNull() }

                    val edges = globalGraph[prefixId]

                    if (counts.size == neighbors.size) {
                        for (i in neighbors.indices) {
                            val targetId = getNodeId(neighbors[i])
                            edges.add(targetId, counts[i])
                            totalEdges += counts[i]
                        }
                    } else {
                        // Fallback if weights are missing
                        for (neighbor in neighbors) {
                            val targetId = getNodeId(neighbor)
                            edges.add(targetId, 1)
                            totalEdges++
                        }
                    }
                }
            }
        }
    }

    println("------ Graph loaded. Unique Nodes: ${globalGraph.size} | Total Edges: $totalEdges")
    println("---- Calculating Eulerian path...")

    val path = hierholzerGlobal()

    println("------ Path calculated. Path length: ${path.size} nodes.")
    println("---- Writing output to $fileOutput...")

    File(fileOutput).also { it.parentFile?.mkdirs() }.bufferedWriter().use { writer ->
        if (path.isNotEmpty()) {
            // Hierholzer's algorithm generates the path backwards, so we iterate down
            writer.write(reverseMap[path.data[path.size - 1]])
            for (i in path.size - 2 downTo 0) {
                writer.write(CONNECTOR.code)
                writer.write(reverseMap[path.data[i]])
            }
            writer.newLine()
        } else {
            writer.write("No path found.")
        }
    }

    // Free memory
    idMap.clear()
    reverseMap.clear()
    globalGraph.clear()

    println("------ Done.")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------
class Ex3 {
    companion object {
        @JvmStatic
        fun run(fileInput: String, fileOutput: String) {
            val time = measureTimeMillis {
                findEulerianPaths(
                    fileInput  = fileInput,
                    fileOutput = fileOutput
                )
            }
            println("Finished finding global Eulerian path in ${time}ms")
        }
    }
}

// -----------------------------------------------------------------------------
// Standalone entry point for direct execution
// -----------------------------------------------------------------------------
fun main() {
    val filePath   = "SRR494099.fastq.gz"
    val fileInput  = "results/Result_2_" + filePath.substring(0, filePath.length-9) + ".csv"
    val fileOutput = "results/Result_3_" + filePath.substring(0, filePath.length-9) + ".csv"
    Ex3.run(fileInput, fileOutput)
}