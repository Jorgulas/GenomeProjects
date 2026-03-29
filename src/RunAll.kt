/**
 * Run all the tests in the project with a [selected file] and [K value].
 */

const val NEED_COUNT_4_Ex2 = false

fun main() {
    val fileInput = "SRR494099.fastq.gz"
    val k = 6

    measureAndPrintTime ("Finished running all exercises") {
        println("// Running Ex1: k-mer counting")
        val ex1Output = "results/Result_1_" + fileInput.substring(0, fileInput.length-9) + ".csv"
        Ex1.run(fileInput, ex1Output, k)
        println()
        println("// Running Ex2: De Bruijn graph construction")
        val ex2Output = "results/Result_2_" + fileInput.substring(0, fileInput.length-9) + ".csv"
        Ex2.run(ex1Output, ex2Output)
        println()
        println("// Running Ex3: Eulerian path finding")
        val ex3Output = "results/Result_3_" + fileInput.substring(0, fileInput.length-9) + ".csv"
        Ex3.run(ex2Output, ex3Output)
        println()
        println("// Running Ex4: Contig extraction")
        val ex4Output = "results/Result_4_" + fileInput.substring(0, fileInput.length-9) + ".csv"
        Ex4.run(ex2Output, ex4Output)
        println()
    }
}
