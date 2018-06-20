//ECE563 Project 2  step 1 OpenMP version   2018/4/25
//Songrui Li	   0025338817 li884@purdue.edu
//              
//note: How to run and test: Enter all the txt files' name you want to test into one txt file named "filename_list.txt". 
//                           Put this .c document and the filename_list.txt file together under the same folder and then run and compile.

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <omp.h>

#define FILENAME_LEN 50
#define FILENAME_NUM 100
#define HASH_TABLE_MAX_SIZE 100000
#define MAX_LINE_SIZE 100000

char text[FILENAME_NUM][MAX_LINE_SIZE * 50];
int hash_size[FILENAME_NUM];
typedef struct HashTable Node;
struct HashTable
{
	char* Key;
	int Value;
	Node* pNext;
};

Node * hashTable[FILENAME_NUM + 1][HASH_TABLE_MAX_SIZE];
unsigned int hashComputeByHashFunc(const char* key)
{
	unsigned int hash = 1;
	int temp;
	while (temp = *key++) 
	{
		hash = hash * 33 + temp;
	}
	return hash;
}


void hashTableInsertFunc(const char* key, int value, int index)
{
	unsigned int pos, poscheck;
	Node* NewNode = (Node*)malloc(sizeof(Node));
	memset(NewNode, 0, sizeof(Node));
	NewNode->Key = (char*)malloc(sizeof(char)* (strlen(key) + 1));
	strcpy(NewNode->Key, key);
	NewNode->Value = value;

	pos = hashComputeByHashFunc(key) % HASH_TABLE_MAX_SIZE;
	poscheck = pos;
	
	while (hashTable[index][poscheck] != NULL && hashTable[index][poscheck]->Value != -1) 
	{
		poscheck++;
		poscheck %= HASH_TABLE_MAX_SIZE;
	}
	NewNode->pNext = hashTable[index][pos];
	hashTable[index][pos] = NewNode;

	hash_size[index]++;
}

Node* hashTableLookupFunc(const char* key, int index)
{
	unsigned int pos = hashComputeByHashFunc(key) % HASH_TABLE_MAX_SIZE;
	Node* pHead;
	if (hashTable[index][pos]) 
	{
		pHead = hashTable[index][pos];
		while (pHead) 
		{
			if (strcmp(key, pHead->Key) == 0) 
			{
				return pHead;
			}
			pHead = pHead->pNext;
		}
	}
	return NULL;
}

void hashTablePrintFunc(int index)
{
	int i = 0;
	Node* pHead;
	printf("=========== content of hash table ===========\n");
	while (i < HASH_TABLE_MAX_SIZE) 
	{
		if (hashTable[index][i]) 
		{
			pHead = hashTable[index][i];
			while (pHead) 
			{
				printf("Word: %s, count: %d ", pHead->Key, pHead->Value);
				pHead = pHead->pNext;
				printf("\n");
			}
		}
		i++;
	}
}


void rmPunct2LowerFunc(char *p)
{
	char *src = p, *dst = p;
	while (*src) 
	{
		if (ispunct((unsigned char)*src) || isdigit((unsigned char)*src)) 
		{
			src++;
		}
		else if (isupper((unsigned char)*src)) 
		{
			*dst++ = tolower((unsigned char)*src);
			src++;
		}
		else if (src == dst) 
		{
			src++;
			dst++;
		}
		else 
		{
			*dst++ = *src++;
		}
	}
	*dst = 0;
}


void readFunc(const char* filename, int index) 
{
	FILE *fp = fopen(filename, "r");
	char word[1000];
	if (fp == NULL)
	{
		printf("input file is invalid !");
		return;
	}
                                                                                                                                                                                        
	while (fscanf(fp, " %s", word) == 1)
	{
		rmPunct2LowerFunc(word);
		strcat(text[index], word);
		strcat(text[index], " ");
	}
	fclose(fp);
}



void mapperFunc(int index) 
{
	char *nextWord;
	nextWord = strtok(text[index], " \r\n");
	while (nextWord != NULL) 
	{
#pragma omp critical 
		{
			if (hashTableLookupFunc(nextWord, index) == NULL) 
			{
				hashTableInsertFunc(nextWord, 1, index);
			}
			else 
			{
				hashTableLookupFunc(nextWord, index)->Value++;
			}
			nextWord = strtok(NULL, " \r\n");
		}
	}
}

void reducerFunc(int reduceCount) 
{
	int i, j;
#pragma omp parallel for
	for (i = 0; i < reduceCount; ++i)
	{
		for (j = 0; j < HASH_TABLE_MAX_SIZE; j++) 
		{
#pragma omp critical
			if (hashTable[i][j])
			{
				Node* pHead = hashTable[i][j];
				while (pHead)
				{
					if (hashTableLookupFunc(pHead->Key, FILENAME_NUM) == NULL)
						hashTableInsertFunc(pHead->Key, pHead->Value, FILENAME_NUM);
					else 
					{
						int val = pHead->Value;
						hashTableLookupFunc(pHead->Key, FILENAME_NUM)->Value += val;
					}
					pHead = pHead->pNext;
				}
			}
		}
	}
}


void writerFunc(FILE * fp) 
{
	int i;
	Node* p;
	fprintf(fp, "------print the result------ \n");
#pragma omp critial 
	{
		for (i = 0; i < HASH_TABLE_MAX_SIZE; ++i)
		{
			if (hashTable[FILENAME_NUM][i] != NULL)
			{
				p = hashTable[FILENAME_NUM][i];
				while (p)
				{
					fprintf(fp, "Word: %s, Count: %d\n", p->Key, p->Value);
					p = p->pNext;
				}
			}
		}
	}
}

// Main Function
int main()
{
	int file_num = 0;
	FILE *read_filename = fopen("filename_list.txt", "r");
	char **filename_list_array = (char **)malloc(sizeof(char*)* FILENAME_NUM);
	int i,j;
	Node* pHead;

#pragma omp parallel for
	for (i = 0; i < FILENAME_NUM; i++)
	{
		filename_list_array[i] = (char *)malloc(sizeof(char)* FILENAME_LEN);
	}

	if (read_filename == NULL)
	{
		printf("open the file incorrectly !");
		return 0;
	}
	while (!feof(read_filename))
	{
		fscanf(read_filename, "%s\n", filename_list_array[file_num]);
		file_num++;
	}
	printf("The result can be found in output.txt .\n");

	omp_set_num_threads(8);
	double time = -omp_get_wtime();
#pragma omp parallel private (i)
	{
#pragma omp single nowait
		{
			for (i = 0; i < file_num; i++) 
			{
#pragma omp task
				{
					readFunc(filename_list_array[i], i);
				}
			}
		}
	}
#pragma omp parallel private (i)
	{
#pragma omp single nowait
		{
			for (i = 0; i < file_num; i++) 
			{
#pragma omp task 
				{
					mapperFunc(i);
				}
			}
		}
	}
#pragma omp barrier
	reducerFunc(file_num);
#pragma omp barrier
	FILE *fp = fopen("output.txt", "w");

	writerFunc(fp);

	hashTablePrintFunc(FILENAME_NUM);

#pragma omp parallel for
	for (i = 0; i < FILENAME_NUM; i++) 
	{
		for (j = 0; j < HASH_TABLE_MAX_SIZE; j++) 
		{
			if (hashTable[i][j]) 
			{
				pHead = hashTable[i][j];
				if (pHead) 
				{
					free(pHead->Key);
					free(pHead);
				}
			}
		}
	}
	
	
	fclose(fp);
	time = time + omp_get_wtime();
	printf("Elapsed time is %lf seconds. \n", time);

	for (i = 0; i < FILENAME_NUM; i++)
	{
		free(filename_list_array[i]);
	}
	free(filename_list_array);

	return 0;
}
