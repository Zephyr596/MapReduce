#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include "mapreduce.h"

// Define the data structure and variable used

// pairs
struct pairs
{
    char *key;
    char *value;
};

//partitions
struct pairs** partitions;

// files
struct  files
{
    char *name;
};

struct files* fileNmaes;

// array for counting partitions
int* pairCountInPartition;
int* pairAllocatedInPartition;
int* numberOfAccessInPartition;

// lock used
pthread_mutex_t lock, fileLock;

// function used
Partitioner p;
Reducer r;
Mapper m;

// variable for counting
int numberPartitions;
int filesProcessed;
int totalFiles;

// Map_thread routine
void* Map_thread(void *arg) 
{
    while (filesProcessed < totalFiles)
    {
        pthread_mutex_lock(&fileLock);
        char* filename = NULL;
        if(filesProcessed < totalFiles)
        {
            filename = fileNmaes[filesProcessed].name;
            filesProcessed++;
        }
        pthread_mutex_unlock(&fileLock);
        if(filename != NULL)
            m(filename);          // call funtion m
    }
    return arg;
}


// Reducer_thread routine
void* Reduce_thread(void *arg)
{
    int* partitionNumber = (int *)arg;
    for(int i = 0; i < pairCountInPartition[*partitionNumber]; i++)
    {
        if(i == numberOfAccessInPartition[*partitionNumber])
        {
            r(partitions[*partitionNumber][i].key, ])
        }
    }
}