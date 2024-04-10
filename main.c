#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include "utils/time_utils.h"
#include "utils/data.h"
#include "utils/analytics.h"

#define MAX_READS 10000

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    // Setup hash tables and ensure they are in the correct order
    HashTable **hash_tables = all_ht_setup();
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
            // Skip first row of the document
            if (!skipped_first) {
                char *starting_line = NULL;
                getline_clean(&starting_line, &rowlen, fp);
                free(starting_line);
                starting_line = NULL;
                skipped_first = 1;
            }

            char **linesToSend = (char **) calloc(size, sizeof(char *));
            int *rowlens = malloc(size * sizeof(int));
            int *displacements = calloc(size, sizeof(int));

            assert(linesToSend);
            assert(rowlens);
            assert(displacements);

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

            // Figure out where each row begins and ends (prefix sum)
            displacements[0] = 0;
            for (int i = 1; i < size && i < ended_size; i++) {
                displacements[i] = displacements[i - 1] + rowlens[i - 1];
            }

            char *strings_to_send = (char *) malloc(sizeof(char) * (displacements[size - 1] + rowlens[size - 1]));
            assert(strings_to_send);
            for (int i = 0; i < size && i < ended_size; i++) {
                memcpy(strings_to_send + displacements[i], linesToSend[i], rowlens[i]);
            }

            if (ended_size < size) {
                for (int i = ended_size; i < size; i++) {
                    displacements[i] = 0;
                    rowlens[i] = 0;
                }
            }

            // Send the row lens to each rank
            MPI_Scatter(rowlens, 1, MPI_INT,
                        &lineRecvLen, 1, MPI_INT,
                        0, MPI_COMM_WORLD);

            if (lineRecvLen == -1) {
                break;
            }

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);

            // Send a line to each rank
            MPI_Scatterv(strings_to_send, rowlens, displacements, MPI_CHAR,
                         lineReceived, lineRecvLen, MPI_CHAR,
                         0, MPI_COMM_WORLD);

            for (int i = 0; i < size && i < ended_size; i++) {
                free(linesToSend[i]);
                linesToSend[i] = NULL;
            }

            free(linesToSend);
            linesToSend = NULL;

            free(strings_to_send);
            strings_to_send = NULL;

            free(rowlens);
            rowlens = NULL;

            free(displacements);
            displacements = NULL;
        } else {
            // Receive the length of the row to be received
            MPI_Scatter(NULL, 1, MPI_INT, &lineRecvLen, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if (lineRecvLen == 0) {
                break;
            }

            lineReceived = malloc(lineRecvLen * sizeof(char));
            assert(lineReceived);

            // Receive the row from the rank
            MPI_Scatterv(NULL, NULL, NULL, MPI_CHAR, lineReceived, lineRecvLen, MPI_CHAR, 0, MPI_COMM_WORLD);
        }

        /**
         *  Deal with line received information
         *      1. convert to data_struct
         *      2. Store into hashtable
         */
        data = read_data_char(lineReceived, lineRecvLen);
        process_tweet_data(hash_tables, all_keys, data);

        free(lineReceived);
        lineReceived = NULL;

        free_data_struct_list(&data);
        data = NULL;
    }

    /**
     *  After the reading of the file has ended and all information has been processed
     *  send Hashtable back to ROOT
     *      1. Commit Type Nodes and create a contiguous type
     *      2. Send it back 
     */
    MPI_Datatype MPI_sentiment_node, MPI_count_node;
    MPI_Datatype MPI_list_size_arr;
    MPI_Datatype sentiment_types[2] = {MPI_CHAR, MPI_LONG_DOUBLE}, count_types[2] = {MPI_CHAR, MPI_INT};
    // Here we just use the longest string possible, found through evaluation of the input with the smaller files
    int lengths[] = {HOUR_STR_LEN, 1};

    // Calculate displacements for sentiment_node
    MPI_Aint displacements[2];
    sentiment_node dummy_sentiment;
    count_node dummy_count;
    MPI_Aint base_address;
    MPI_Get_address(&dummy_sentiment, &base_address);
    MPI_Get_address(&dummy_sentiment.key[0], &displacements[0]);
    MPI_Get_address(&dummy_sentiment.sentiment, &displacements[1]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, sentiment_types, &MPI_sentiment_node);
    MPI_Type_commit(&MPI_sentiment_node);

    // Calculate displacements of count_node
    MPI_Get_address(&dummy_count, &base_address);
    MPI_Get_address(&dummy_count.key[0], &displacements[0]);
    MPI_Get_address(&dummy_count.count, &displacements[1]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, count_types, &MPI_count_node);
    MPI_Type_commit(&MPI_count_node);

    // 0 -> happy hour, 1 -> happy day, 2 -> active hour, 3 -> active day
    int hash_list_sizes[] = {hash_tables[0]->size, hash_tables[1]->size, hash_tables[2]->size, hash_tables[3]->size};
    MPI_Type_contiguous(4, MPI_INT, &MPI_list_size_arr);
    MPI_Type_commit(&MPI_list_size_arr);

    items *items_to_send = all_ht_to_lists(hash_tables, all_keys);

    if (rank == 0) {
        /**
         * Handle strings and store information
         *      1. Aggregate and find_most();
         *      2. Clean up hashtable and linked list of keys
        */
        sentiment_node *temp_happy_hour;
        sentiment_node *temp_happy_day;
        count_node *temp_active_hour;
        count_node *temp_active_day;

        // Allocate memory for receiving the sizes of the lists of other processors
        int *list_sizes = malloc(size * 4 * sizeof(int));
        int **list_size_transpose = (int **) malloc(sizeof(int *) * 4);

        for (int i = 0; i < 4; i++) {
            list_size_transpose[i] = (int *) malloc(sizeof(int) * size);
        }

        MPI_Gather(hash_list_sizes, 1, MPI_list_size_arr,
                   list_sizes, 1, MPI_list_size_arr,
                   0, MPI_COMM_WORLD);

        // Each index corresponds to:
        // 0 -> happy hour, 1 -> happy day, 2 -> active hour, 3 -> active day
        int temp_sizes[] = {0, 0, 0, 0};

        // Aggregate the sizes to be used later in memory allocation for the arrays that will receive the data
        for (int i = 0; i < size; i++) {
            temp_sizes[0] += list_sizes[4 * i + 0]; // happy hour
            temp_sizes[1] += list_sizes[4 * i + 1]; // happy day
            temp_sizes[2] += list_sizes[4 * i + 2]; // active hour
            temp_sizes[3] += list_sizes[4 * i + 3]; // active day

            list_size_transpose[0][i] = list_sizes[4 * i + 0];
            list_size_transpose[1][i] = list_sizes[4 * i + 1];
            list_size_transpose[2][i] = list_sizes[4 * i + 2];
            list_size_transpose[3][i] = list_sizes[4 * i + 3];
        }

        int *happy_hour_displacements = calloc(size, sizeof(int));
        int *happy_day_displacements = calloc(size, sizeof(int));
        int *active_hour_displacements = calloc(size, sizeof(int));
        int *active_day_displacements = calloc(size, sizeof(int));
        happy_hour_displacements[0] = 0;
        happy_day_displacements[0] = 0;
        active_hour_displacements[0] = 0;
        active_day_displacements[0] = 0;

        for (int i = 1; i < size; i++) {
            happy_hour_displacements[i] = happy_hour_displacements[i - 1] + list_sizes[4 * (i - 1) + 0];
            happy_day_displacements[i] = happy_day_displacements[i - 1] + list_sizes[4 * (i - 1) + 1];
            active_hour_displacements[i] = active_hour_displacements[i - 1] + list_sizes[4 * (i - 1) + 2];
            active_day_displacements[i] = active_day_displacements[i - 1] + list_sizes[4 * (i - 1) + 3];
        }

        temp_happy_hour = (sentiment_node *) malloc(sizeof(sentiment_node) * temp_sizes[0]);
        temp_happy_day = (sentiment_node *) malloc(sizeof(sentiment_node) * temp_sizes[1]);
        temp_active_hour = (count_node *) malloc(sizeof(count_node) * temp_sizes[2]);
        temp_active_day = (count_node *) malloc(sizeof(count_node) * temp_sizes[3]);

        // Get the hash tables from the other ranks
        MPI_Gatherv(items_to_send->happy_hour, list_sizes[0], MPI_sentiment_node,
                    temp_happy_hour, list_size_transpose[0], happy_hour_displacements, MPI_sentiment_node,
                    0, MPI_COMM_WORLD);

        MPI_Gatherv(items_to_send->happy_day, list_sizes[1], MPI_sentiment_node,
                    temp_happy_day, list_size_transpose[1], happy_day_displacements, MPI_sentiment_node,
                    0, MPI_COMM_WORLD);

        MPI_Gatherv(items_to_send->active_hour, list_sizes[2], MPI_count_node,
                    temp_active_hour, list_size_transpose[2], active_hour_displacements, MPI_count_node,
                    0, MPI_COMM_WORLD);

        MPI_Gatherv(items_to_send->active_day, list_sizes[3], MPI_count_node,
                    temp_active_day, list_size_transpose[3], active_day_displacements, MPI_count_node,
                    0, MPI_COMM_WORLD);

        /**
         * Deal with each incoming MPI nodes, be sure to read in the string properly.
         * This needs to be in the form fn(root_hashtable, root_keys, data_type, void *node_type)
        */
        if (size > 1) {
            consolidate_child_data(hash_tables[0], all_keys[0], 1, (void *) temp_happy_hour, temp_sizes[0],
                                   happy_hour_displacements[1]);
            consolidate_child_data(hash_tables[1], all_keys[1], 1, (void *) temp_happy_day, temp_sizes[1],
                                   happy_day_displacements[1]);
            consolidate_child_data(hash_tables[2], all_keys[2], 0, (void *) temp_active_hour, temp_sizes[2],
                                   active_hour_displacements[1]);
            consolidate_child_data(hash_tables[3], all_keys[3], 0, (void *) temp_active_day, temp_sizes[3],
                                   active_day_displacements[1]);
        }
        free(temp_happy_hour);
        free(temp_happy_day);
        free(temp_active_hour);
        free(temp_active_day);


        char *final_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        char *final_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        find_most(hash_tables[0], all_keys[0], final_happy_hour, 1);
        find_most(hash_tables[1], all_keys[1], final_happy_day, 1);
        find_most(hash_tables[2], all_keys[2], final_active_hour, 0);
        find_most(hash_tables[3], all_keys[3], final_active_day, 0);

        printf("Happiest Hour: %s, sentiment: %.2LF\n", final_happy_hour,
               *(long double *) ht_lookup(hash_tables[0], final_happy_hour));
        printf("Happiest Day: %s, sentiment: %.2LF\n", final_happy_day,
               *(long double *) ht_lookup(hash_tables[1], final_happy_day));
        printf("Most Active Hour: %s, count: %d\n", final_active_hour,
               *(int *) ht_lookup(hash_tables[2], final_active_hour));
        printf("Most Active Day: %s, count: %d\n", final_active_day,
               *(int *) ht_lookup(hash_tables[3], final_active_day));

        free(list_size_transpose);
        fclose(fp);
    } else {
        /**
         * Handle strings and store information
         *      1. Send length of the items to root
         *      2. Send items back to root
        */
        MPI_Gather(hash_list_sizes, 1, MPI_list_size_arr,
                   NULL, 0, MPI_list_size_arr,
                   0, MPI_COMM_WORLD);
        MPI_Gatherv(items_to_send->happy_hour, hash_list_sizes[0], MPI_sentiment_node,
                    NULL, NULL, NULL, MPI_sentiment_node,
                    0, MPI_COMM_WORLD);
        MPI_Gatherv(items_to_send->happy_day, hash_list_sizes[1], MPI_sentiment_node,
                    NULL, NULL, NULL, MPI_sentiment_node,
                    0, MPI_COMM_WORLD);
        MPI_Gatherv(items_to_send->active_hour, hash_list_sizes[2], MPI_count_node,
                    NULL, NULL, NULL, MPI_count_node,
                    0, MPI_COMM_WORLD);
        MPI_Gatherv(items_to_send->active_day, hash_list_sizes[3], MPI_count_node,
                    NULL, NULL, NULL, MPI_sentiment_node,
                    0, MPI_COMM_WORLD);

    }

    MPI_Finalize();
    return 0;
}