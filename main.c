#include <mpi.h>
#include <stdio.h>

#define MAX_READS 10000

int main(int argc, char** argv) {

    // Initialise struct key_stats *key_stats

    int ierr, total_reads;
    ierr = MPI_Init(&argc, &argv);

    // Initialise struct ret_struct *ret_struct
    int my_id;
    int num_cores;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
    MPI_Comm_size(MPI_COMM_WORLD, &num_cores);

    if (num_cores == 1) {

    }

    
    MPI_Finalize();
}