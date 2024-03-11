#include <mpi.h>
#include <stdio.h>
#include "utils/time_utils.h"
#include "utils/data.h"

#define MAX_READS 10000

int main(int argc, char** argv) {

    // Initialise struct key_stats *key_stats

    int ierr, total_reads;
    size_t size = 0;
    FILE *fp = fopen(argv[1], "r"); // We will need to make sure of a certain format to use argv[1], please run in this format: mpirun -np 1 ./build/main ../data/twitter-1mb.json
    // ierr = MPI_Init(&argc, &argv);

    char *starting_line = NULL;
    getline_clean(&starting_line, &size, fp); // TODO: please make sure to read in rows from this string!!!
    free(starting_line);
    starting_line = NULL;

    // Initialise struct ret_struct *ret_struct
    int my_id;
    int num_cores = 1;
    // MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
    // MPI_Comm_size(MPI_COMM_WORLD, &num_cores);

    if (num_cores == 1) {
        data_struct *new_data = read_data(fp, MAX_READS);
        data_struct *next = new_data;
        for (int i=0;i < MAX_READS;i++) {
            if (next) {
                printf(
                    "%d) time: %d-%d-%d %d:%d:%d, sentiment: %LF \n",
                    i,
                    next->time->year,
                    next->time->month,
                    next->time->day,
                    next->time->hour,
                    next->time->minute,
                    next->time->seconds,
                    next->sentiment
                );
                next = next->next;
            }
        }
        free_data_struct_list(&new_data); // Temporary free here for testing
    }
    else {
        // Multiple cores to work on processing data
    }

    fclose(fp);
    // MPI_Finalize();
}