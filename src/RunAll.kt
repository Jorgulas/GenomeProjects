/**
 * Run all the tests in the project with a [selected file] and [K value].
 */

const val NEED_COUNT_4_Ex2 = false

fun main() {
    val fileInput = "SRR494099.fastq.gz"
    val filename = fileInput.substringBefore(".fastq.gz")
    val k = 3

    // HowManyRepetitiveExecutionForCalculationOfAverage
    // !! Deixe como 1 para apenas uma execução !!
    //      :: -- A menos que queira obter a média de tempos de execução
    val hmrefcoa = 1

    measureAverageTime(hmrefcoa, filename) {
        measureAndPrintTime ("") {
            println("// Running Ex1: k-mer counting")
            val ex1Output = "results/Result_1_$filename.csv"
            Ex1.run(fileInput, ex1Output, k)
            println()

            println("// Running Ex2: De Bruijn graph construction")
            val ex2Output = "results/Result_2_$filename.csv"
            Ex2.run(ex1Output, ex2Output)
            println()

            println("// Running Ex3: Eulerian path finding")
            val ex3Output = "results/Result_3_$filename.csv"
            Ex3.run(ex2Output, ex3Output)
            println()

            println("// Running Ex4: Contig extraction")
            val ex4Output = "results/Result_4_$filename.csv"
            Ex4.run(ex2Output, ex4Output)
            println()
        }
    }
}
