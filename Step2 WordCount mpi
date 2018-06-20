//ECE563 Project 2 step 2 MPI Version Final Turnin Version
//Songrui Li    0025338817 li884@purdue.edu
//             
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <omp.h>
#define HASH_TABLE_MAX_SIZE 10000
#define MAX_LINE_SIZE 100000
#define MAX_FILE_NUM 32
#define WORD_NUM 40

int flag[2];
int global_count = 0;
char text[MAX_LINE_SIZE * WORD_NUM];
int hash_table_size;  //the number of key-value pairs in the hash table!

typedef struct HashTable Node;
struct HashTable
{
    char key[40];
    int nValue;
    Node* pNext;
};

Node* hashTable[MAX_FILE_NUM][HASH_TABLE_MAX_SIZE * WORD_NUM]; //hash table data strcutrue

// hash function
unsigned int hash_table_hash_str(const char* key)
{
    const signed char *p = (const signed char*)key;
    unsigned int hash = 5381;
    int temp;
    while( temp = *key++) {
        hash = hash * 33 + temp;
    }
    return hash;
}

//insert key-value into hash table
void hash_table_insert(const char* key, int nvalue, int index)
{
    unsigned int pos = hash_table_hash_str(key) % HASH_TABLE_MAX_SIZE;
    Node* pNewNode = (Node*)malloc(sizeof(Node));
    memset(pNewNode, 0, sizeof(Node));
	if(hash_table_size >= HASH_TABLE_MAX_SIZE)
    {
        return;
    }
    strcpy(pNewNode->key, key);
    pNewNode->nValue = nvalue;
    hashTable[index][pos] = pNewNode;
    hash_table_size++;
}

//lookup a key in the hash table
Node* hash_table_lookup(const char* key, int index)
{
    unsigned int pos = hash_table_hash_str(key) % HASH_TABLE_MAX_SIZE;
	Node* pHead;
    if(hashTable[index][pos])
    {
        pHead = hashTable[index][pos];
        if (pHead)
        {
            if(strcmp(key, pHead->key) == 0)
                return pHead;
        }
    }
    return NULL;
}

void clean_data(char *p)
{
    char *src = p, *dst = p;
    while (*src) {
        if (ispunct((unsigned char)*src)) {
            src++;
        } else if (isdigit((unsigned char)*src)) {
            src++;
        } else if (isupper((unsigned char)*src)) {
            *dst++ = tolower((unsigned char)*src);
            src++;
        } else if (src == dst) {
            src++;
            dst++;
        } else {
            *dst++ = *src++;
        }
    }
    *dst = 0;
}


// Reader function 
void reader(const char* filename) 
{
  FILE *fp = fopen(filename, "r");
  char word[1024] = {0};
  if(fp == NULL){
    printf("invalid input file");
    exit(1);
  }
  //read word by word                                                                                                                                                                                        
  while(fscanf(fp, " %1023s", word) == 1){
    //convert to lower case                                                                                                                                                                                  
    clean_data(word);
    strcat(text, word);
    strcat(text," ");
    global_count++;
  }
  fclose(fp);
}


// Mapping function 
void Mapping(int step_one, int step_previous, int pid) {
    char *nextWord;
    char * ptr = NULL;
    int count = 0;
    nextWord = strtok_r(text, " \r\n", &ptr);
    while(nextWord != NULL) {
        if(count < pid * step_one + step_previous) {
            nextWord = strtok_r(NULL, " \r\n", &ptr);
        }
        else 
			break;
        count++;
    }
    count = 0;
    while (nextWord != NULL && count <= pid * step_one + step_previous) {
    #pragma omp critical
        if (hash_table_lookup(nextWord, pid) == NULL) {
            hash_table_insert(nextWord, 1, pid); 
        } else {
             hash_table_lookup(nextWord, pid)->nValue++;   
        }
        count++;
        nextWord = strtok_r(NULL, " \r\n", &ptr);
    }
}

// Reducer function
void reducer(int r_count) {
    int i, j, val;
	Node* pHead;
    #pragma parallel for
    for(i = 0; i < r_count; ++i)
    {
        for (j = 0; j < HASH_TABLE_MAX_SIZE; j++) {
            #pragma omp critical
            if(hashTable[i][j])
            {
                pHead = hashTable[i][j];
                if(pHead)
                {
                    if (hash_table_lookup(pHead->key, MAX_FILE_NUM) == NULL)
                        hash_table_insert(pHead->key, pHead->nValue, MAX_FILE_NUM);
                    else {
                        val = pHead -> nValue;
                        hash_table_lookup(pHead->key, MAX_FILE_NUM)->nValue += val;
                    }
                    
                }
            }
        }
    }
}

//writer function
void writer(FILE * f) {
    int i, j;
	Node* p;
    fprintf(f, "-------- Printing out the result -------\n");
    #pragma omp critical
    for(i = 0; i < HASH_TABLE_MAX_SIZE*WORD_NUM; i++)
       {
        if(hashTable[0][i] != NULL)
        {
          p = hashTable[0][i];
          if(p)
            {
              fprintf(f, "Word: %s, Count: %d\n", p->key, p->nValue);
            }
        }
    }
}
/*------------------------main function ----------------------*/
int main(int argc, char** argv) {
    // Initialize the MPI environment
    int numP, pid,j;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numP);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Status status[16];
    MPI_Request request[16];
    int nthreads, tid;
    double start=MPI_Wtime();
	
	hash_table_size = 0;
    memset(hashTable, 0, sizeof(Node*) * HASH_TABLE_MAX_SIZE); //fill hashtable with zero
    int i = 0;

    MPI_Datatype Particletype;
    MPI_Datatype type[2] = { MPI_INT, MPI_BYTE};
    int len_block[2] = {40, 1};
    MPI_Aint offsets[2];
    offsets[0] = 0;
    offsets[1] = 40;
    MPI_Type_create_struct(2, len_block, offsets, type, &Particletype);//creat MPI datatype
    MPI_Type_commit(&Particletype);

    int step_one = 0;
    int step_previous = 0;
    //start READER
    for(i = 0; i < argc - 1; i++) {
        reader(argv[i+1]);
    }
    //calculate for steps and step_previous
    step_one = global_count / numP;
    step_previous = global_count % numP;
    
    if (pid == 0) {
        //broadcast text
        Node rec[numP][100000];
        //initialization
        int k = 0;
        char * temp = " ";
        for (i = 0; i < numP; i++) {
            for (k = 0; k < 100000; k++) {
                rec[i][k].nValue = 0;

                strcpy(rec[i][k].key, temp);
            }
        }
        MPI_Bcast(text, MAX_LINE_SIZE * WORD_NUM, MPI_BYTE, 0, MPI_COMM_WORLD);
        //Mapping
        char *nextWord;
        char * ptr = NULL;
        int count = 0;
        nextWord = strtok_r(text, " \r\n", &ptr);
        while (nextWord != NULL && count < step_one + step_previous) {
            #pragma omp critical 
            {
                if (hash_table_lookup(nextWord, 0) == NULL) {
                    hash_table_insert(nextWord, 1, 0); 
                } else {
                    hash_table_lookup(nextWord, 0)->nValue++;   
                }
                count++;
                nextWord = strtok_r(NULL, " \r\n", &ptr);
            }
        }
        i = 1;
        while(i < numP) {

            MPI_Irecv(&rec[i], 100000, Particletype, i, i, MPI_COMM_WORLD, request+i-1);
            i++;
        }
        MPI_Waitall(numP-1, request, status);
        //reduce
        int j = 0;
        for (i = 1; i < numP; i++) {
            for (j = 0; j < 100000; j++) {
                if(rec[i][j].nValue > 0) {
                    if (hash_table_lookup(rec[i][j].key, 0) == NULL) {
                    hash_table_insert(rec[i][j].key, rec[i][j].nValue, 0); 
                    } else {
                        hash_table_lookup(rec[i][j].key, 0)->nValue += rec[i][j].nValue;   
                    }
                }
            }
        }

        double end=MPI_Wtime();
        //writer thread
        FILE *fp = fopen("output2.txt", "w");
        writer(fp); 
        printf("time: %f \n", end - start);
    }
    else if(pid != 0) {
        MPI_Bcast(text, MAX_LINE_SIZE * WORD_NUM, MPI_BYTE, 0, MPI_COMM_WORLD);
        //Mapping
        Node send[100000];
        int index = pid;
        int count = 0;
        char* key;
        Mapping(step_one, step_previous, pid);
        //copy to send
        for(i = 0; i < HASH_TABLE_MAX_SIZE; ++i)
        if(hashTable[index][i])
        {
            Node* pHead = hashTable[index][i];
            if (pHead)
            {
                send[count].nValue = pHead->nValue;
                key = pHead->key;
                strcpy(send[count].key, key);
                count++;
            }
        }
        MPI_Send(&send, count, Particletype, 0, pid, MPI_COMM_WORLD);
    }
	
	j = 0;
	for(i = 0; i < HASH_TABLE_MAX_SIZE; ++i)//free the memory of the hash table
    {
        if(j >= 31) break;
        if(hashTable[j][i])
        {
            j++;
            Node* pHead = hashTable[j][i];
            if (pHead)
            {
                Node* pTemp = pHead;
                if(pTemp)
                {
                    free(pTemp->key);
                    free(pTemp);
                }
            }
        }
    }

    // Finalize the MPI environment.
    MPI_Finalize();
    
}
