#include <mpi.h>
#include <stdio.h>
#include "utils/time_utils.h"
#include "utils/data.h"
#include "utils/analytics.h"

#define MAX_READS 10000

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    FILE *fp = NULL;
    size_t rowlen = 0;
    if (rank == 0) {
        fp = fopen(argv[1], "r");
    }
    int8_t end = 0;
    while (end != -1) {
        int lineRecvLen = 0;
        char *lineReceived;
        if (rank == 0) {
            // Skip first row
            char *starting_line = NULL;
            getline_clean(&starting_line, &rowlen, fp);
            free(starting_line);
            starting_line = NULL;

            char **linesToSend = malloc(size * sizeof(char *));
            int *rowlens = malloc(size * sizeof(int));
            int *displacements = calloc(size, sizeof(int));

            for (int i = 0; i < size; i++) {
                linesToSend[i] = NULL;
                rowlens[i] = 0;
                end = getline_clean(&linesToSend[i], &rowlen, fp);
                rowlens[i] = (int) rowlen;
                if (end == -1) break;
            }

            // figure out where each row begins and ends (prefix sum)
            displacements[0] = 0;
            for (int i = 1; i <= size; i++) {
                displacements[i] = displacements[i - 1] + rowlens[i - 1];
            }
            MPI_Scatter(rowlens, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);
            lineReceived = malloc(lineRecvLen * sizeof(char));

            MPI_Scatterv(*linesToSend, rowlens, displacements, MPI_CHAR, &lineReceived, lineRecvLen, MPI_CHAR, 0,
                         MPI_COMM_WORLD);
            for (int i = 0; i < size; i++) {
                free(linesToSend[i]);
            }
            free(linesToSend);
            free(rowlens);
            free(displacements);
        } else {
            MPI_Scatter(NULL, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);
            lineReceived = malloc(lineRecvLen * sizeof(char));
            MPI_Scatterv(NULL, NULL, NULL, MPI_CHAR, &lineReceived, lineRecvLen, MPI_CHAR, 0, MPI_COMM_WORLD);
        }
        printf("Rank %d received line: %s \n", rank, lineReceived);
        free(lineReceived);
    }



//    ret_struct *ret_struct = NULL;
//
//    printf("rank:%d of %d\n", rank, wsize);
//    data_struct *new_data = read_data(fp, MAX_READS);
//    ret_struct = process_tweets(new_data);
//    printf("Most Happy Hour: %s, sentiment: %.2LF\n", ret_struct->happy_hour_time, ret_struct->happy_hour_sentiment);
//    printf("Most Happy Day: %s, sentiment: %.2LF\n", ret_struct->happy_day_time, ret_struct->happy_day_sentiment);
//    printf("Most Active Hour: %s, count: %d\n", ret_struct->active_hour_time, ret_struct->active_hour_count);
//    printf("Most Active Day: %s, count: %d\n", ret_struct->active_day_time, ret_struct->active_day_count);
//    free_ret_struct(&ret_struct);
//    free_data_struct_list(&new_data); // Temporary free here for testing

    fclose(fp);
    // MPI_Finalize();
}