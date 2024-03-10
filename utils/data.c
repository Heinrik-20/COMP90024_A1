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

data_struct *read_data(FILE *fp, int max_reads) {

    data_struct *new_data = (data_struct *) malloc(sizeof(data_struct));
    char *line_ptr = NULL;
    for (int counter = 0; counter < max_reads; counter ++) {
        // The getline_clean() calls here will start from the second line onwards
        // From here onwards, all getline_clean() calls will only read the json lines
        if (getline(&line_ptr, MAX_CHAR, fp) != (-1)) {
            // process json, extract key stats
        }
        
    }

}