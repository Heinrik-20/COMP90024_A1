#include <assert.h>
#include "data.h"

ssize_t getline_clean(char **line_ptr, size_t *n, FILE *stream){
	ssize_t ret_val = getline(line_ptr, n, stream); // reallocs if line is too long
    if(ret_val != (-1) && strlen(*line_ptr) > 0 && 
       (*line_ptr)[strlen(*line_ptr) - 1] == '\n'){
        (*line_ptr)[strlen(*line_ptr) - 1] = '\0';
    }
    if(ret_val != (-1) && strlen(*line_ptr) > 0 && 
       (*line_ptr)[strlen(*line_ptr) - 1] == '\r'){
        (*line_ptr)[strlen(*line_ptr) - 1] = '\0';
    }
    return ret_val;
}

data_struct *alloc_data_struct() {
    data_struct *new_data = (data_struct *) malloc(sizeof(data_struct));
    assert(new_data);
    new_data->next = NULL;
    return new_data;
}

data_struct *read_data(FILE *fp, int max_reads) {

    data_struct *head_data = NULL;
    data_struct *temp = NULL;
    data_struct *new_data = alloc_data_struct();

    char *line_ptr = NULL;
    char time_storage[20];
    char sentiment_storage[MAX_CHAR];
    char *sentiment = NULL;
    int index = 0;
    size_t size = 0;

    for (int counter = 0; counter < max_reads; counter ++) {
        // The getline_clean() calls here will start from the second line onwards
        // From here onwards, all getline_clean() calls will only read the json lines
        if (getline_clean(&line_ptr, &size, fp) != (-1)) {
            // process json, extract key stats: stats only is doc->data->created_at & doc->data->sentiment
            memcpy(time_storage, strstr(line_ptr, "\"created_at\":") + 14, 20);
            time_storage[19] = '\0';
            sentiment = strstr(line_ptr, "\"sentiment\":");
            if (!sentiment) {
                // Indicate that there was no read of the sentiment.
                sentiment_storage[0] = '-';
                sentiment_storage[1] = '9';
                sentiment_storage[2] = '9';
                sentiment_storage[3] = '9';
                sentiment_storage[4] = '9';
                index = 5;

            } else {
                    sentiment += 12;

                    if (sentiment[0] == '-') {
                        sentiment_storage[0] = '-';
                        index += 1;
                    }
                    
                    while ((isdigit(sentiment[index])) || (sentiment[index] == '.')) {
                        sentiment_storage[index] = sentiment[index];
                        index += 1;
                    }

                    if (index == 0) {
                        // Indicate that there was no read of the sentiment.
                        sentiment_storage[0] = '-';
                        sentiment_storage[1] = '9';
                        sentiment_storage[2] = '9';
                        sentiment_storage[3] = '9';
                        sentiment_storage[4] = '9';
                        index = 5;
                    }
            }
            sentiment_storage[index] = '\0';
            new_data->time = make_time(time_storage);
            new_data->sentiment = strtold(sentiment_storage, NULL);
            index = 0;

            free(line_ptr);
            line_ptr = NULL;
        }

        temp = alloc_data_struct();
        if (counter == 0) {
            head_data = new_data;
            head_data->next = temp;
            new_data = temp;
        } else {
            new_data->next = temp;
            new_data = temp;
        }
        
    }

    return head_data;

}

void free_data_struct_list(data_struct **list) {

    data_struct *data_to_free = *list;
    while (data_to_free) {
        data_struct *next = data_to_free->next;
        free(data_to_free); // TODO: will need to free the time_struct_t before doing this
        data_to_free = NULL;
        assert(data_to_free == NULL);
        data_to_free = next; 
    }

    return;
}