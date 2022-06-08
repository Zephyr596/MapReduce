#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"


void Map(char *file_name)
{
    // printf("Map function has started...\n");
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    while (getline(&line, &size, fp) != -1)
    {
        char *token, *dummy = line;
        // printf("dummy:%s\n", dummy);
        while ((token = strsep(&dummy, " .?!;-+,\t\n\r\0")) != NULL && (*token != '\n') && (*token != '\r') && (*token != '\0'))
        {
            // printf("token:%s\n",token);
            // printf("token(int):%d",*token);
            // printf("dummy:%s\n", dummy);
            MR_Emit(token, "1");
        }
        // printf("break!!!\n");
    }
    free(line);
    fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number)
{
    // FILE *fp = fopen("out.txt", "a");
    // if(fp == NULL)
    // {
    //     exit(1);
    // }
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
        count++;
    // fprintf(fp,"%s %d\n", key, count);
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[])
{
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}