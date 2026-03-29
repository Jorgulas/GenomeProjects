/**
 * O objetivo deste exercício é obter os contigs de um grafo de Bruijn.
 * Um contig é um caminho não ramificado máximo num grafo de Bruijn.
 * Um caminho é considerado não ramificado se in(v) = out(v) = 1 para cada nó intermediário v desse caminho,
 * ou seja, para cada nó, exceto possivelmente o nó inicial e final de um caminho.
 * Um caminho não ramificado máximo é um caminho não ramificado que não pode ser estendido para um caminho não
 * ramificado mais longo.
 */

import java.io.File


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
fun findContigs(fileInput: String, fileOutput: String) {
    println("---- Loading entire Global Graph into memory...")
    val globalGraph = HashMap<String, HashSet<String>>()

    // 1. Load the entire graph
    File(fileInput).useLines { lines ->
        lines.forEach { line ->
            if (line.isNotBlank()) {
                val arrowIdx = line.indexOf(CONNECTOR)
                val semiIdx = line.indexOf(SEPARATOR)
                if (arrowIdx != -1) {
                    val prefix = line.substring(0, arrowIdx)
                    // Read up to the semicolon to ignore the weights
                    val suffixStr = if (semiIdx != -1) line.substring(arrowIdx + 1, semiIdx)
                    else line.substring(arrowIdx + 1)

                    val neighbors = HashSet<String>()
                    suffixStr.split(DELIMITER).forEach { if (it.isNotBlank()) neighbors.add(it) }
                    globalGraph[prefix] = neighbors
                }
            }
        }
    }

    println("------ Graph loaded. Nodes: ${globalGraph.size}")
    println("---- Extracting contigs...")

    // 2. Run your existing getContigs function ONCE on the full graph
    val contigs = getContigs(globalGraph)

    // 3. Write output
    File(fileOutput).bufferedWriter().use { writer ->
        contigs.forEach {
            writer.write(it)
            writer.newLine()
        }
    }
    println("------ Finished extracting ${contigs.size} contigs.")
}

// -----------------------------------------------------------------------------
// Entry point
// -----------------------------------------------------------------------------
class Ex4 {
    companion object {
        fun run(fileInput: String, fileOutput:String) {
            measureAndPrintTime("Finished extracting contigs") {
                findContigs(
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
    val fileOutput = "results/Result_4_"+ filePath.substring(0, filePath.length-9) + ".csv"
    Ex4.run(fileInput,fileOutput)
}