#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include "mapreduce.h"

#define MAXNPAIRS 1024
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
struct  file
{
    char *name;
};

struct file* fileNmaes;

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

//    get_next function
char* get_next(char *key, int partition_number)
{
    int num = numberOfAccessInPartition[partition_number];
    if(num < pairCountInPartition[partition_number] && 
        strcmp(key, partitions[partition_number][num].key) == 0)
    {
        numberOfAccessInPartition[partition_number]++;
        return partitions[partition_number][num].value;
    }else
    {
        return NULL;
    }
}

void* Reduce_thread(void *arg)
{
    int* partitionNumber = (int *)arg;
    for(int i = 0; i < pairCountInPartition[*partitionNumber]; i++)
    {
        if(i == numberOfAccessInPartition[*partitionNumber])
        {
            r(partitions[*partitionNumber][i].key, get_next, *partitionNumber);
        }
    }
    return arg;
}// End of Reduce_Thread

// Compare function
//     Sort the buckets by key and then by value
int compare(const void* pairs1, const void* pairs2)
{
    struct pairs *p1 = (struct pairs*)pairs1;
    struct pairs *p2 = (struct pairs*)pairs2;

    if(strcmp(p1->key, p2->key) == 0)
    {
        return strcmp(p1->value, p2->value);
    }

    return strcmp(p1->key, p2->key);
}

//      Sort files by increasing size
int compareFiles(const void *pairs1,const void *pairs2)
{
    struct file *f1 = (struct file*)pairs1;
    struct file *f2 = (struct file*)pairs2;

    struct stat st1,st2;
    stat(f1->name, &st1);
    stat(f2->name, &st2);
    long int size1 = st1.st_size;
    long int size2 = st2.st_size;
    
    return (size1 - size2);
}

void MR_Emit(char *key, char *value)
{
    pthread_mutex_lock(&lock);

    // Get partition_number
    unsigned long hashPartitionNumber = p(key, numberPartitions);
    pairCountInPartition[hashPartitionNumber]++;
    int curCount = pairCountInPartition[hashPartitionNumber];
    // Checking if allocated memory has been exceed
    if(curCount > pairAllocatedInPartition[hashPartitionNumber])
    {
        pairAllocatedInPartition[hashPartitionNumber] *= 2;
        partitions[hashPartitionNumber] = (struct pairs *)
        realloc(partitions[hashPartitionNumber], pairAllocatedInPartition[hashPartitionNumber] * sizeof(struct pairs));
    }
    partitions[hashPartitionNumber][curCount - 1].key = (char *)malloc((strlen(key) + 1) * sizeof(char));
    strcpy(partitions[hashPartitionNumber][curCount - 1].key, key);
    partitions[hashPartitionNumber][curCount - 1].value = (char *)malloc((strlen(value) + 1) * sizeof(char));
    strcpy(partitions[hashPartitionNumber][curCount - 1].value, value);

    pthread_mutex_unlock(&lock);
}

void Init(
    Mapper map, int num_mappers, 
    Reducer reduce, int num_reducers, 
    Partitioner partition, int fileNumbers,
    int *arrayPosition)
{
    p = partition;
    m = map;
    r = reduce;
    numberPartitions = num_reducers;
    partitions = (struct pairs**)malloc(num_reducers * sizeof(struct pairs *));
    fileNmaes = (struct file*)malloc(fileNumbers * sizeof(struct file));
    pairCountInPartition = (int *)malloc(num_reducers * sizeof(int));
    pairAllocatedInPartition = (int *)malloc(num_mappers * sizeof(int));
    numberOfAccessInPartition = (int *)malloc(num_mappers * sizeof(int));

    filesProcessed = 0;
    totalFiles = -1;

    // Initialising the arrays needed to store the key value pairs in the partitions
    for(int i = 0; i < num_mappers; i++)
    {
        partitions[i] = (struct pairs *)malloc(MAXNPAIRS * sizeof(struct pairs));
        pairCountInPartition[i] = 0;
        pairAllocatedInPartition[i] = 1024;
        arrayPosition[i] = i;
        numberOfAccessInPartition[i] = 0;
    }


}

void SortFiles(int fileNumber, char *FileNameArray[])
{
    for(int i = 0; i < fileNumber; i++)
    {
        fileNmaes[i].name = (char *)malloc(strlen(FileNameArray[i] + 1) * sizeof(char));
        strcpy(fileNmaes[i].name, FileNameArray[i]);
    }

    qsort(&fileNmaes[0], fileNumber - 1, sizeof(struct file), compareFiles);
}

void SortPartitons(int partitionNUmber)
{
    for(int i = 0; i < partitionNUmber; i++)
    {
        qsort(partitions[i], pairCountInPartition[i], sizeof(struct pairs), compare);
    }

}
void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    if(argc - 1 < num_mappers) {
        num_mappers = argc - 1;
    }

    // Initialising all variables
    pthread_t mapThreads[num_mappers];
    pthread_t reduceThreads[num_reducers];
    pthread_mutex_init(&lock, NULL);
    pthread_mutex_init(&fileLock, NULL);
    int arrayPosition[num_reducers];

    Init(map, num_mappers, reduce, num_reducers, partition, argc - 1, arrayPosition);

    SortFiles(argc - 1, argv+1);

    // Creating the threads for the number of mappers
    for(int i = 0; i < num_mappers; i++)
    {
        pthread_create(&mapThreads[i], NULL, Map_thread, NULL);
    }

    // Waiting for threads to merge

    for(int i = 0; i < num_mappers; i++)
    {
        pthread_join(mapThreads[i], NULL);
    }

    SortPartitions(num_reducers);

    // Creating the threads for the number of reducers
    for(int i = 0; i < num_reducers; i++)
    {
        // if (pthread_create(&reduceThreads[i], NULL, Reduce_thread, &arrayPosition[i]))
        // {
        //     printf("Error!\n");
        // }
        pthread_create(&reduceThreads[i], NULL, Reduce_thread, &arrayPosition[i]);
    }

    // Waiting for the threads to merge
    for(int i = 0; i < num_reducers; i++)
    {
        pthread_join(reduceThreads[i], NULL);
    }

    //MapReduce End

    // Destory lock
    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&fileLock);

    // Freeing partitions
    for(int i = 0; i < num_mappers; i++)
    {
        // Freeing pairs
        for(int j = 0; j < pairCountInPartition[i]; j++)
        {
            if(partitions[i][j].key != NULL || partitions[i][j].value != NULL)
            {
                free(partitions[i][j].key);
                free(partitions[i][j].value);
            }
        }

        free(partitions[i]);
    }

    // Freeing filenames
    for(int i = 0; i < argc - 1; i++)
    {
        free(fileNmaes[i].name);
    }

    // Freeing memory
    free(partitions);
    free(fileNmaes);
    free(pairCountInPartition);
    free(pairAllocatedInPartition);
    free(numberOfAccessInPartition);
}

// Default hash function

unsigned long MRDefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while((c = *key++) != '\0')
    {
        hash = hash*33 + c;
    }

    return hash % num_partitions;
}