#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<pthread.h>
#include<string.h>
#include<assert.h>
#include"mapreduce.h"

#define NUM_MAPS 1024

pthread_mutex_t fileLock;
Partitioner p;

int NUM_REDUCER;
int NUM_FILES;
int counter;

char **fileNames;

Mapper m;
Reducer r;

typedef struct __v_node {
    char *value;
    struct __v_node *next;
} v_node;

typedef struct __k_node {
    char *key;
    v_node *head;
    struct __k_node *next;
} k_node;

typedef struct __k_entry {
    k_node *head;
    pthread_mutex_t lock;
} k_entry;

typedef struct __p_entry {
    k_entry map[NUM_MAPS];
    int key_num;
    pthread_mutex_t lock;
    k_node *sorted;
    int cur_visit;
} p_entry;

p_entry hm[64];

void init(int argc, char* argv[], Mapper mapper, int num_reducers, Partitioner partition, Reducer reducer_arg) {
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
    for(int i = 0; i < NUM_REDUCER; i++) {
        pthread_mutex_init(&hm[i].lock, NULL);
        hm[i].key_num = 0;
        hm[i].sorted = NULL;
        hm[i].cur_visit = 0;
        for(int j = 0; j < NUM_MAPS; j++) {
            hm[i].map[j].head = NULL;
            pthread_mutex_init(&hm[i].map[j].lock, NULL);
        }
    }
}


char* get_func(char *key, int partition_number) {
    k_node *arr = hm[partition_number].sorted;
    char *value;
    while(1) {
        int cur = hm[partition_number].cur_visit;
        if(strcmp(arr[cur].key, key) == 0) {
            if(arr[cur].head == NULL) {
                return NULL;
            }
            v_node *temp = arr[cur].head->next;
            value = arr[cur].head->value;
            arr[cur].head = temp;
            return value;
        }
        else {
            hm[partition_number].cur_visit++;
            continue;
        }
        return NULL;
    }
}

void* Map_thread(void *arg){
    while (1)
    {
        char *filename;
        pthread_mutex_lock(&fileLock);
        if(counter >= NUM_FILES) {
            pthread_mutex_unlock(&fileLock);
            return NULL;
        }
        filename = fileNames[counter++];
        pthread_mutex_unlock(&fileLock);
        m(filename);
    }
    
}

int compareStr(const void *a, const void *b) {
    char *n1 = ((k_node *)a)->key;
    char *n2 = ((k_node *)b)->key;
    return strcmp(n1, n2);
}

void *Reduce_thread(void *arg) {
    int partition_number = *(int *) arg;
    free(arg);
    arg = NULL;
    if(hm[partition_number].key_num == 0) {
        return NULL;
    }
    hm[partition_number].sorted = malloc(hm[partition_number].key_num * sizeof(k_node));
    int count = 0;
    for(int i = 0; i < NUM_MAPS; i++) {
        k_node *cur = hm[partition_number].map[i].head;
        if(cur == NULL) {
            continue;
        }
        while(cur != NULL) {
            hm[partition_number].sorted[count] = *cur;
        }
    }
}