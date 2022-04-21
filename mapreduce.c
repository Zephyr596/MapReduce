#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include "mapreduce.h"

#define NUM_MAPS 1024

pthread_mutex_t fileLock;
Partitioner p;

int NUM_REDUCER;
int NUM_FILES;
int counter;

char **fileNames;

Mapper m;
Reducer r;

typedef struct __v_node
{
    char *value;
    struct __v_node *next;
} v_node;

typedef struct __k_node
{
    char *key;
    v_node *head;
    struct __k_node *next;
} k_node;

typedef struct __k_entry
{
    k_node *head;
    pthread_mutex_t lock;
} k_entry;

typedef struct __p_entry
{
    k_entry map[NUM_MAPS];
    int key_num;
    pthread_mutex_t lock;
    k_node *sorted;
    int cur_visit;
} p_entry;

p_entry hm[64];

void init(int argc, char *argv[], Mapper mapper, int num_reducers, Partitioner partition, Reducer reducer_arg)
{
    int rc = pthread_mutex_init(&fileLock, NULL);
    assert(rc == 0);
    counter = 0;
    p = partition;
    NUM_REDUCER = num_reducers;

    NUM_FILES = argc - 1;
    fileNames = (argv + 1);
    m = mapper;
    r = reducer_arg;

    // Hash initialization
    for (int i = 0; i < NUM_REDUCER; i++)
    {
        pthread_mutex_init(&hm[i].lock, NULL);
        hm[i].key_num = 0;
        hm[i].sorted = NULL;
        hm[i].cur_visit = 0;
        for (int j = 0; j < NUM_MAPS; j++)
        {
            hm[i].map[j].head = NULL;
            pthread_mutex_init(&hm[i].map[j].lock, NULL);
        }
    }
}

char *get_func(char *key, int partition_number)
{
    k_node *arr = hm[partition_number].sorted;
    char *value;
    while (1)
    {
        int cur = hm[partition_number].cur_visit;
        if (strcmp(arr[cur].key, key) == 0)
        {
            if (arr[cur].head == NULL)
            {
                return NULL;
            }
            v_node *temp = arr[cur].head->next;
            value = arr[cur].head->value;
            arr[cur].head = temp;
            return value;
        }
        else
        {
            hm[partition_number].cur_visit++;
            continue;
        }
        return NULL;
    }
}

void *Map_thread(void *arg)
{
    while (1)
    {
        char *filename;
        pthread_mutex_lock(&fileLock);
        if (counter >= NUM_FILES)
        {
            pthread_mutex_unlock(&fileLock);
            return NULL;
        }
        filename = fileNames[counter++];
        pthread_mutex_unlock(&fileLock);
        m(filename);
    }
}

int compareStr(const void *a, const void *b)
{
    char *n1 = ((k_node *)a)->key;
    char *n2 = ((k_node *)b)->key;
    return strcmp(n1, n2);
}

void *Reduce_thread(void *arg)
{
    int partition_number = *(int *)arg;
    free(arg);
    arg = NULL;
    if (hm[partition_number].key_num == 0)
    {
        return NULL;
    }
    hm[partition_number].sorted = malloc(hm[partition_number].key_num * sizeof(k_node));
    int count = 0;
    for (int i = 0; i < NUM_MAPS; i++)
    {
        k_node *cur = hm[partition_number].map[i].head;
        if (cur == NULL)
        {
            continue;
        }
        while (cur != NULL)
        {
            hm[partition_number].sorted[count] = *cur;
            count++;
            cur = cur->next;
        }
    }

    qsort(hm[partition_number].sorted, hm[partition_number].key_num, sizeof(k_node), compareStr);

    for (int i = 0; i < count; i++)
    {
        printf("%s", hm[partition_number].sorted[i].key);
    }

    for (int i = 0; i < count; i++)
    {
        char *key = hm[partition_number].sorted[i].key;
        r(key, get_func, partition_number);
    }

    // TODO free the data on heap
    // TODO free all the nodes
    for (int i = 0; i < NUM_MAPS; i++)
    {
        k_node *cur = hm[partition_number].map[i].head;
        if (cur == NULL)
            continue;
        while (cur != NULL)
        {
            free(cur->key);
            v_node *vcur = cur->head;
            while (vcur != NULL)
            {
                free(vcur->value);
                vcur->value = NULL;
                v_node *temp = vcur->next;
                free(vcur);
                vcur = temp;
            }
            vcur = NULL;
            k_node *tempK = cur->next;
            free(cur);
            cur = tempK;
        }
        cur = NULL;
    }
    free(hm[partition_number].sorted);
    hm[partition_number].sorted = NULL;

    return NULL;
}

void MR_Emit(char *key, char *value)
{
    unsigned long partition_number = p(key, NUM_REDUCER);
    unsigned long map_number = MR_DefaultHashPartition(key, NUM_MAPS);
    pthread_mutex_lock(&hm[partition_number].map[map_number].lock);
    k_node *temp = hm[partition_number].map[map_number].head;
    while (temp != NULL)
    {
        if (strcmp(temp->key, key) == 0)
        {
            break;
        }
        temp = temp->next;
    }

    v_node *new_v = malloc(sizeof(v_node));
    if (new_v == NULL)
    {
        perror("malloc");
        pthread_mutex_unlock(&hm[partition_number].map[map_number].lock);
        return;
    }
    new_v->value = malloc(sizeof(char) * 20);
    if (new_v->value == NULL)
        printf("ERROR MALLOC FOR VALUE");
    strcpy(new_v->value, value);
    new_v->next = NULL;

    // If there is no existing node for same key
    if (temp == NULL)
    {
        k_node *new_key = malloc(sizeof(k_node));
        if (new_key == NULL)
        {
            perror("malloc");
            pthread_mutex_unlock(&hm[partition_number].map[map_number].lock);
            return;
        }

        new_key->head = new_v;
        new_key->next = hm[partition_number].map[map_number].head;
        hm[partition_number].map[map_number].head = new_key;

        new_key->key = malloc(sizeof(char) * 20);
        if (new_key->key == NULL)
            printf("ERROR MALLOC FOR VALUE");
        strcpy(new_key->key, key);
        pthread_mutex_lock(&hm[partition_number].lock);
        hm[partition_number].key_num++;
        pthread_mutex_unlock(&hm[partition_number].lock);
    }
    else
    {
        // If ther is exsiting node for same key
        new_v->next = temp->head;
        temp->head = new_v;
    }

    pthread_mutex_unlock(&hm[partition_number].map[map_number].lock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition)
{
    init(argc, argv, map, num_reducers, partition, reduce);

    // Create map threads
    pthread_t mapthreads[num_mappers];
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_create(&mapthreads[i], NULL, Map_thread, NULL);
    }

    // join waits for the threads to finish
    for (int k = 0; k < num_mappers; k++)
    {
        pthread_join(mapthreads[k], NULL);
    }

    // create reduce threads
    pthread_t reducethreads[num_reducers];
    for (int j = 0; j < num_reducers; j++)
    {
        void *arg = malloc(4);
        *(int *)arg = j;
        pthread_create(&reducethreads[j], NULL, Reduce_thread, arg);
    }

    for (int m = 0; m < num_reducers; m++)
    {
        pthread_join(reducethreads[m], NULL);
    }
}