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
     *      1. Commit Type Nodes and create a contiguous type
     *      2. Send it back 
     */
    MPI_Datatype sentiment_node, count_node;
    MPI_Datatype sentiment_hour_arr, sentiment_day_arr, count_hour_arr, count_day_arr;
    MPI_Datatype list_size_arr;
    MPI_Datatype sentiment_types[2] = {MPI_LONG_DOUBLE, MPI_CHAR}, count_types = {MPI_INT, MPI_CHAR};
    int lengths = {1, HOUR_STR_LEN}; // Here we just use the longest string we can find
    MPI_Aint displacements[2];

    MPI_Sentiment_node dummy_sentiment;
    MPI_Count_node dummy_count;
    MPI_Aint base_address;

    MPI_Get_address(&dummy_sentiment, &base_address);
    MPI_Get_address(&dummy_sentiment.key[0], &displacements[1]);
    MPI_Get_address(&dummy_sentiment.sentiment, &displacements[0]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, sentiment_types, &sentiment_node);
    MPI_Type_commit(&sentiment_node);

    MPI_Get_address(&dummy_count, &base_address);
    MPI_Get_address(&dummy_count.key[0], &displacements[1]);
    MPI_Get_address(&dummy_count.count, &displacements[0]);

    displacements[0] = MPI_Aint_diff(displacements[0], base_address);
    displacements[1] = MPI_Aint_diff(displacements[1], base_address);

    MPI_Type_create_struct(2, lengths, displacements, count_types, &count_node);
    MPI_Type_commit(&count_node);

    MPI_Type_contiguous(hash_tables[0]->size, sentiment_node, &sentiment_hour_arr);
    MPI_Type_commit(&sentiment_hour_arr);
    MPI_Type_contiguous(hash_tables[1]->size, sentiment_node, &sentiment_day_arr);
    MPI_Type_commit(&sentiment_day_arr);
    MPI_Type_contiguous(hash_tables[2]->size, count_node, &count_hour_arr);
    MPI_Type_commit(&count_hour_arr);
    MPI_Type_contiguous(hash_tables[3]->size, count_node, &count_day_arr);
    MPI_Type_commit(&count_day_arr);

    int list_sizes[] = {hash_tables[0]->size, hash_tables[1]->size, hash_tables[2]->size, hash_tables[3]->size};
    MPI_Type_contiguous(4, MPI_INT, &list_size_arr);

    MPI_Items *items_to_send = all_ht_to_lists(hash_tables, all_keys);

    if (rank == 0) {
        
        /**
         * TODO: Handle strings and store informations
         *      1. Implement string_to_ht()
         *      2. Store additional new keys, and new scores (sentiment score or active counts)
         *      3. Aggregate and find_most();
         *      4. Clean up hashtable and linkedlist of keys
        */
        MPI_Sentiment_node *temp_happy_hour;
        MPI_Sentiment_node *temp_happy_day;
        MPI_Count_node *temp_active_hour;
        MPI_Count_node *temp_active_day;
        for (int i=1;i < size;i++) {
            MPI_Recv(list_sizes, 1, list_size_arr, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(temp_happy_hour, 1, sentiment_hour_arr, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(temp_happy_day, 1, sentiment_day_arr, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(temp_active_hour, 1, count_hour_arr, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(temp_active_day, 1, count_day_arr, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Deal with each incoming MPI nodes, be sure to read in the string properly.
            /**
             * This needs to be in the form fn(root_hashtable, root_keys, data_type, void *node_type)
            */
            consolidate_child_data(hash_tables[0], all_keys[0], 1, (void *)temp_happy_hour, list_sizes[0]);    
            consolidate_child_data(hash_tables[1], all_keys[1], 1, (void *)temp_happy_day, list_sizes[1]);    
            consolidate_child_data(hash_tables[2], all_keys[2], 0, (void *)temp_active_hour, list_sizes[2]);    
            consolidate_child_data(hash_tables[3], all_keys[3], 0, (void *)temp_active_day, list_sizes[3]);
        
        }
        char *final_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        char *final_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
        char *final_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
        find_most(hash_tables[0], all_keys[0], final_happy_hour, 1);
        find_most(hash_tables[1], all_keys[1], final_happy_day, 1);
        find_most(hash_tables[2], all_keys[2], final_active_hour, 1);
        find_most(hash_tables[3], all_keys[3], final_active_day, 1);

        printf("Most Happy Hour: %s, sentiment: %.2LF\n", final_happy_hour, *(long double *)ht_lookup(hash_tables[0], final_happy_hour));
        printf("Most Happy Day: %s, sentiment: %.2LF\n", final_happy_day, *(long double *)ht_lookup(hash_tables[1], final_happy_day));
        printf("Most Active Hour: %s, count: %d\n", final_active_hour, *(int *)ht_lookup(hash_tables[2], final_active_hour));
        printf("Most Active Day: %s, count: %d\n", final_active_day, *(int *)ht_lookup(hash_tables[3], final_active_day));


        fclose(fp);
    } else {
        /**
         * TODO: Handle strings and store informations
         *      1. Send items back to root
         *      2. Clean up hashtable and linkedlist of keys
        */
        MPI_Send(list_sizes, 1, list_size_arr, 0, 0, MPI_COMM_WORLD);
        MPI_Send(items_to_send->happy_hour, 1, sentiment_hour_arr, 0, 0, MPI_COMM_WORLD);
        MPI_Send(items_to_send->happy_day, 1, sentiment_day_arr, 0, 0, MPI_COMM_WORLD);
        MPI_Send(items_to_send->active_hour, 1, count_hour_arr, 0, 0, MPI_COMM_WORLD);
        MPI_Send(items_to_send->active_day, 1, count_day_arr, 0, 0, MPI_COMM_WORLD);

    }

    MPI_Finalize();
    return 0;

}