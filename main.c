#include <mpi.h>
#include <stdio.h>
#include "utils/time_utils.h"
#include "utils/data.h"

#define MAX_READS 10000

int main(int argc, char** argv) {

    // Initialise struct key_stats *key_stats

    int ierr, total_reads;
    size_t size = 0;
    FILE *fp = fopen(argv[3], "r");
    ierr = MPI_Init(&argc, &argv);

    char *starting_line = (char *)malloc(sizeof(char) * MAX_CHAR);
    getline_clean(&starting_line, &size, fp); // TODO: please make sure to read in rows from this string!!!

    // Initialise struct ret_struct *ret_struct
    int my_id;
    int num_cores;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_id);
    MPI_Comm_size(MPI_COMM_WORLD, &num_cores);

    if (num_cores == 1) {
        data_struct *new_data = read_data(fp, 100);
        for (int i=0;i < 100;i++) {
            printf(
                "time: %d-%d-%d %d:%d:%d, sentiment: %LF \n",
                new_data->time->year,
                new_data->time->month,
                new_data->time->day,
                new_data->time->hour,
                new_data->time->minute,
                new_data->time->seconds,
                new_data->sentiment
            );
            new_data = new_data->next;
        }
    }
    else {
        // Multiple cores to work on processing data
    }

    
    MPI_Finalize();
}