# Algorithms in Bioinformatics

### This repository contains implementations of various algorithms used in bioinformatics, including K-Mer extraction, De Bruijn Graph construction, Eulerian Path constructor and Contig Detector.

**Files**:  
* [Ex1.kt](src/Ex1.kt) - Contains the implementation of K-Mer extraction.
* [Ex2.kt](src/backup/Ex2_old.kt) - Contains the implementation of De Bruijn Graph construction.
* [Ex3.kt](src/Ex3.kt) - Contains the implementation of Eulerian Path constructor.
* [Ex4.kt](src/Ex4.kt) - Contains the implementation of Contig Detector.
* [QoL_Functions.kt](src/QoL_Functions.kt) - Contains "quality of life" functions used across the implementations.

### Usage
To Start, you need a file from 
[ENA Browser](https://www.ebi.ac.uk/ena/browser/text-search?query=SRR494099) or 
[SRA Browser](https://www.ncbi.nlm.nih.gov/sra?term=SRR494099) 
(or any other source of FASTQ files).   
So the Ex1 implementation can extract K-Mers from the FASTQ file.

> [!NOTE]  
> The dependencies within:  
> * Ex2.kt depends on the Ex1.kt's compilation.
> * Ex3.kt depends on Ex2.kt's compilation.
> * Ex4.kt depends on Ex2.kt's compilation.


