#include <mpi.h>
#include <stdio.h>
#include "utils/time_utils.h"
#include "utils/data.h"
#include "utils/analytics.h"

#define MAX_READS 10000

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);

    HashTable **hash_tables = all_ht_setup(); // This ensures that the proper order of the Hashtables
    key_list **all_keys = all_keys_setup();

    int rank;
    int size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    FILE *fp = NULL;
    size_t rowlen = 0;
    int skipped_first = 0;

    if (rank == 0) {
        fp = fopen(argv[1], "r");
    }

    data_struct *data = NULL;
    int end = 0;
    int ended_size = -1;
    while (end != -1) {
        int lineRecvLen = 0;
        char *lineReceived;
        if (rank == 0) {
            // Skip first row
            if (!skipped_first) {
                char *starting_line = NULL;
                getline_clean(&starting_line, &rowlen, fp);
                free(starting_line);
                starting_line = NULL;
                skipped_first = 1;
            }
            
            char **linesToSend = malloc(size * sizeof(char *));
            int *rowlens = malloc(size * sizeof(int));
            int *displacements = calloc(size, sizeof(int));

            assert (linesToSend);
            assert (rowlens);
            assert (displacements);

            for (int i = 0; i < size; i++) {
                linesToSend[i] = NULL;
                rowlens[i] = 0;
                end = getline_clean(&linesToSend[i], &rowlen, fp);
                rowlens[i] = (int) rowlen;
                if (end == -1) {
                    ended_size = i;
                    break;
                } else {
                    ended_size = i + 1;
                }
            }

            // figure out where each row begins and ends (prefix sum)
            displacements[0] = 0;
            for (int i = 0; i < size && i < ended_size; i++) {
                displacements[i] = displacements[i - 1] + rowlens[i - 1];
            }

            char *string_to_send = (char *) malloc(sizeof(char) * (displacements[size - 1] + rowlens[size - 1]));
            assert(string_to_send);
            for (int i = 0; i < size && i < ended_size; i++) {
                memcpy(string_to_send + displacements[i], linesToSend[i], rowlens[i]);
            }

            if (ended_size < size) {
                for (int i=ended_size;i < size;i++) {
                    displacements[i] = -1;
                    rowlens[i] = -1;

                    free(linesToSend[i]);
                    linesToSend[i] = NULL;

                    printf("Ended size less than total size\n");
                }
            }

            MPI_Scatter(rowlens, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);
            MPI_Scatterv(string_to_send, rowlens, displacements, MPI_CHAR, lineReceived, lineRecvLen, MPI_CHAR, 0,
                         MPI_COMM_WORLD);

            for (int i = 0; i < size && i < ended_size; i++) {
                free(linesToSend[i]);
                linesToSend[i] = NULL;
            }

            free(linesToSend);
            linesToSend = NULL;

            free(string_to_send);
            string_to_send = NULL;

            free(rowlens);  
            rowlens = NULL;

            free(displacements);
            displacements = NULL;


        } else {
            MPI_Scatter(NULL, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if (lineRecvLen == -1) {
                break;
            }

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);

            MPI_Scatterv(NULL, NULL, NULL, MPI_CHAR, lineReceived, lineRecvLen, MPI_CHAR, 0, MPI_COMM_WORLD);
        }
        
        /**
         *  TODO: Deal with line received information
         *      1. convert to data_struct
         *      2. Store into hashtable
         */
        
        data = read_data_char(lineReceived, lineRecvLen);
        process_tweet_data(hash_tables, all_keys, data);

        free(lineReceived);
        lineReceived = NULL;
        
    }

    /**
     *  TODO: Send Hashtable back to ROOT 
     *      1. Implement ht_to_string()
     */
    

    if (rank == 0) {
        
        /**
         * TODO: Handle strings and store informations
         *      1. Implement string_to_ht()
         *      2. Store additional new keys, and new scores (sentiment score or active counts)
         *      3. Aggregate and find_most();
         *      4. Clean up hashtable and linkedlist of keys
        */

        fclose(fp);
    } else {
        /**
         * TODO: Handle strings and store informations
         *      1. Clean up hashtable and linkedlist of keys
        */
    }

    MPI_Finalize();
    return 0;

}