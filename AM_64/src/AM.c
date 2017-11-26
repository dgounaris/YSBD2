#include "AM.h"
#include "bf.h"
#include "defn.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Blocks in the B+ tree are either index blocks or data blocks.
 *
 * Index Blocks:
 * *--------------------------*---------------------------------*----------------------------------*-----------------*
 * | Character - ('i' or 'd') | Integer - Current Block Counter | Integer - Pointer to lower level | Void* SortValue |
 * *--------------------------*---------------------------------*----------------------------------*-----------------*
 *
 * Data Blocks:
 * *--------------------------*----------------------------------*-----------------*--------------*
 * | Character - ('i' or 'd') | Integer - Current Record Counter | Void * - Value1 | Void* Value2 |
 * *--------------------------*----------------------------------*-----------------*--------------*
 *
 * */

int AM_errno = AME_OK;

void AM_Init() {
    //make sure every global data structure is initialized empty
    int i=0;
    BF_Init(MRU);
    for (;i<20;i++) {
        fileTable[i].fileName = NULL;
        fileTable[i].fileIndex = -1;
        
        scanTable[i].scanFile = -1;
        scanTable[i].scanNextBlock = -1;
        scanTable[i].scanNextOffset = -1;
    }
	return;
}


int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {
    int fileDesc;
    BF_Block *block, *datablock0, *indexblock0;

    if(attrType1 == 'c' && attrLength1 > 255* sizeof(char))
        return -1;
    else if(attrType1 == 'f' && attrLength1 > sizeof(float))
        return -1;
    else if(attrType1 == 'i' && attrLength1 > sizeof(int))
        return -1;

    if(BF_CreateFile(fileName) == BF_OK){
        if(BF_OpenFile(fileName,&fileDesc) == BF_OK) {
            BF_Block_Init(&block);
            BF_AllocateBlock(fileDesc, block);
            char data = 'd', index = 'i';
            int curBlockSize = 0;
            int firstDataBlock = 1;
            int nextDataBlock = -1;
            //Value for identifying father block. Change Scan.
            int previousBlock = 2;

            BF_Block_Init(&datablock0);
            BF_AllocateBlock(fileDesc, datablock0);
            char *initblockData = BF_Block_GetData(datablock0);
            memcpy(initblockData, &data, sizeof(char));
            memcpy(initblockData + sizeof(char), &curBlockSize, sizeof(int));                           //Incremental value for records in block
            //Value for identifying father block. Change Scan.
            memcpy(initblockData + sizeof(char) + sizeof(int), &previousBlock, sizeof(int));
            memcpy(initblockData + BF_BLOCK_SIZE - sizeof(int), &nextDataBlock, sizeof(int));

            BF_Block_Init(&indexblock0);
            BF_AllocateBlock(fileDesc, indexblock0);
            char *initblockIndexData = BF_Block_GetData(indexblock0);
            memcpy(initblockIndexData, &index, sizeof(char));
            memcpy(initblockIndexData + sizeof(char), &curBlockSize, sizeof(int));                      //Incremental value for records in block
            int rootHasNoFather = -1;
            memcpy(initblockIndexData + sizeof(char) + sizeof(int), &rootHasNoFather, sizeof(int));
            memcpy(initblockIndexData + sizeof(char) + sizeof(int) + sizeof(int), &firstDataBlock, sizeof(int));

            char *blockData = BF_Block_GetData(block);
            memcpy(blockData,&attrType1, sizeof(char));                                                 //Writing attributes in the first block.
            memcpy(blockData + sizeof(char),&attrLength1, sizeof(int));
            memcpy(blockData + sizeof(char) + sizeof(int),&attrType2, sizeof(char));
            memcpy(blockData + sizeof(char) + sizeof(int) + sizeof(char),&attrLength2, sizeof(int));
            BF_Block_SetDirty(block);                                                               //Changed data, setting dirty, unpinning and freeing pointer to block.
            BF_UnpinBlock(block);
            BF_Block_Destroy(&block);
        } else {
            BF_PrintError(BF_OpenFile(fileName,&fileDesc));
            return BF_ERROR;
        }
    } else {
        BF_PrintError(BF_CreateFile(fileName));
        return BF_ERROR;
    }
    return AME_OK;
}

int AM_DestroyIndex(char *fileName) {
    //check if file is open
    int i=0;
    for (;i<20;i++) {
        if (strcmp(fileTable[i].fileName, fileName)==0) {
        return 1; //file is open
        }
    }
    remove(strcat("./",fileName)); //stdio.h function to remove file
    return AME_OK;
}

int AM_OpenIndex (char *fileName) {
    int fileDesc;
    if(BF_OpenFile(fileName,&fileDesc)==BF_OK){                         //Opening File and checking if it has opened correctly.
        for(int i=0; i<20; i++){                                        //For each record in the fileTable array, check if the fileIndex is unchanged (cont.)
            if(fileTable[i].fileIndex == -1){                           //(which means there is no open file that's taking that spot.) If it is, make the index equal to i.
                fileTable[i].fileIndex = i;
                fileTable[i].fileName = fileName;
                return fileTable[i].fileIndex;
            }
        }
    } else {
        BF_PrintError(BF_OpenFile(fileName,&fileDesc));
        return BF_ERROR;
    }
}


int AM_CloseIndex (int fileDesc) {
    //Debug Start
    for(int i=0; i<20; i++){
        if(fileTable[i].fileIndex == fileDesc && strcmp(fileTable[i].fileName,"EMP-NAME") == 0){
            int blocksNum;
            BF_Block *initblock;
            BF_Block_Init(&initblock);
            if(BF_GetBlock(fileDesc,0,initblock)!=BF_OK){
                return -1;
            }
            char *initblockData = BF_Block_GetData(initblock);
            int size1, size2;
            memcpy(&size1, initblockData + sizeof(char), sizeof(int));
            memcpy(&size2, initblockData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));
            BF_GetBlockCounter(fileDesc,&blocksNum);
            for(int j=1; j<blocksNum;j++){
                BF_Block *block;
                BF_Block_Init(&block);
                if(BF_GetBlock(fileDesc,j,block)!=BF_OK){
                    return -1;
                }
                char *blockData = BF_Block_GetData(block);
                if(blockData[0]=='i'){
                    printf("Index Block Records: %d | Block Father: %d | Block Number: %d |\n", blockData[1],blockData[5],j);
                    int curRecords;
                    memcpy(&curRecords,blockData + sizeof(char), sizeof(int));
                    for(int k=0;k<curRecords;k++){
                        int inttollevel;
                        void *value = malloc(size1);
                        memcpy(&inttollevel,blockData + sizeof(char) + 2* sizeof(int) + k* sizeof(int) + k* size1, sizeof(int));
                        memcpy(value,blockData + sizeof(char) + 2* sizeof(int) + (k+1)* sizeof(int) + k* size1, size1);
                        printf("Block int to lower level: %d | Block Sort Value: %s |\n", inttollevel, (char*)value);
                    }
                }
                else if(blockData[0]=='d'){
                    printf("Data Block Records: %d | Block Father: %d | Block Number: %d\n", blockData[1],blockData[5],j);
                    int curRecords;
                    memcpy(&curRecords,blockData + sizeof(char), sizeof(int));
                    for(int k=0;k<curRecords;k++){
                        void *value1 = malloc(size1),*value2 = malloc(size2);
                        memcpy(value1,blockData + sizeof(char) + 2* sizeof(int) + k* size1 + k* size2, size1);
                        memcpy(value2,blockData + sizeof(char) + 2* sizeof(int) + (k+1)* size1 + k* size2, size2);
                        printf("Block Value1: %s | Block Value2: %d |\n", (char*)value1, *(int*)value2);
                    }
                }
            }
        }
    }

    //Debug End
    if(BF_CloseFile(fileDesc) == BF_OK){                                    //Closing File and checking if it has closed successfully.
        for(int i=0; i<20; i++){                                            //For each record in the fileTable array, check if the fileIndex is equal to the fileDesc.
                                                                            //If it is, reset the struct variables to the values they had during the struct's initialization.
            if(fileTable[i].fileIndex == fileDesc){
                fileTable[i].fileIndex = -1;
                fileTable[i].fileName = NULL;
            }
        }
    }
    return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
    for(int i=0;i<20;i++) {
        if (fileTable[i].fileIndex == fileDesc) {
            BF_Block* curBlock;
            BF_Block_Init(&curBlock);

            char type1,type2;
            int size1,size2,currentBlock = 0;                                           //The block with the type definition and length is block #0.
            int currentOffset = sizeof(char);                                           //Every block has 1 header byte
            if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
                return -1;
            }

            char *typeData = BF_Block_GetData(curBlock);                                //Getting Type and Size of the Key-Value pair.
            memcpy(&type1, typeData, sizeof(char));
            memcpy(&size1, typeData + sizeof(char), sizeof(int));
            memcpy(&type2, typeData + sizeof(char) + sizeof(int), sizeof(char));
            memcpy(&size2, typeData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));
            BF_UnpinBlock(curBlock);

            currentBlock = 2;                                                          //Top index block is block #2
            if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
                return -1;
            }
            char *indexData = BF_Block_GetData(curBlock);

            //TRAVERSING TREE.
            int maxRecords;
            while (indexData[0]=='i'){                                                                                  //While the block is an index...
                int sizeOfSortValueToPass = 0;
                memcpy(&maxRecords,indexData + sizeof(char), sizeof(int));                                              //Store the current number of records in the block in the maxRecords variable.
                int j;

                for(j=0;j<maxRecords;j++) { //maxrecords counts the delimiters in index block                                                                         //For each record in the block...
                    sizeOfSortValueToPass = sizeof(char) + sizeof(int) + (2*j+1)*sizeof(int);//changed from j+1 to j+2                           //Check Index Block Definition
                    void *sortValue;
                    memcpy(sortValue, indexData + sizeOfSortValueToPass, size1);                             //SortValue could be and int,char or float, so just void.
                    //printf("SortValue: %s\n", (char*)sortValue);
                    if(scanOpCodeHelper(value1, sortValue, type1)){                                                                            //scanOpCodeHelper is used to check if sortValue is larger or smaller than the value passed as argument.
                        //printf("In for j < Max Records: %d ||", maxRecords);
                        //Moving down a level, making indexData equal to the new block's index data.
                        //printf("moving down\n");
                        int lowerLevelFileDesc;
                        memcpy(&lowerLevelFileDesc,indexData + sizeof(char) + sizeof(int) + sizeof(int) + (2*j) * sizeof(int), sizeof(int));           //lowerLevelFileDesc is used as a pointer to the next level of index blocks.
                        if (BF_GetBlock(fileDesc, lowerLevelFileDesc, curBlock)!=BF_OK) {
                            return -1;
                        }
                        indexData = BF_Block_GetData(curBlock);
                        break;
                    }
                }
                if(j==maxRecords){
                    //printf("In for j == Max Records: %d ||", maxRecords);
                    //Moving down a level, making indexData equal to the new block's index data.
                    //printf("moving down\n");
                    int lowerLevelFileDesc;
                    memcpy(&lowerLevelFileDesc,indexData + sizeof(char) + sizeof(int) + sizeof(int) + (2*j) * sizeof(int), sizeof(int));//changed from j to j+1                //lowerLevelFileDesc is used as a pointer to the next level of index blocks.
                    //printf("%d |||| %d\n",  sizeof(char) + sizeof(int) + (2*j) * sizeof(int), lowerLevelFileDesc);
                    //printf("%c%d%d\n", indexData[0], indexData[1], indexData[5]);
                    if (BF_GetBlock(fileDesc, lowerLevelFileDesc, curBlock)!=BF_OK) {
                        return -1;
                    }
                    indexData = BF_Block_GetData(curBlock);
                }
            }
            //INSERTING DATA TO NEW BLOCK.
            //changed: using size1, size2 instead of sizeof(varType)
            if(indexData[0]=='d'){                                                                                      //No more index blocks, only data blocks.
                int curRecords;
                //printf("%d %d\n", size1, size2);
                memcpy(&curRecords,indexData+sizeof(char), sizeof(int));
                //printf("In data, record %d\n", curRecords);
                //printf("Total R %d\n", curRecords);
                int currentBlockSize = sizeof(char) + 2*sizeof(int) + curRecords* size1 + curRecords* size2 + sizeof(int);
                if((currentBlockSize + size1 + size2) <= BF_BLOCK_SIZE){
                    memcpy(indexData + sizeof(char) + 2*sizeof(int) + curRecords* size1 + curRecords* size2,value1,size1);
                    memcpy(indexData + sizeof(char) + 2*sizeof(int) + curRecords* size1 + curRecords* size2 + size1,value2,size2);
                    curRecords++;
                    memcpy(indexData + sizeof(char),&curRecords, sizeof(int));
                    //need to sort the block, probably here
                    SortBlock(indexData, size1, type1, size2,curRecords);
                    BF_Block_SetDirty(curBlock);
                    BF_UnpinBlock(curBlock);
                }
                else {
                    //printf("overflow\n");
                    SplitBlock(value1, value2, indexData,fileDesc,size1,size2,type1);
                    //TODO - Split Block
                }
                return AME_OK;
            }
        }
    }
    return AME_OK;
}

int SplitBlock(void *value1, void *value2, char* blockData,int fileDesc, int size1, int size2, char type1){
    BF_Block* newBlock;
    BF_Block_Init(&newBlock);
    BF_AllocateBlock(fileDesc, newBlock);
    if(blockData[0] == 'd'){
        //printf("In split for data\n");
        char data = 'd';
        char *newBlockData = BF_Block_GetData(newBlock);
        //Initializing new block.
        memcpy(newBlockData,&data, sizeof(char));
        //memcpy(newBlockData + sizeof(char),&curRecords, sizeof(int));
        int blockDataFather,oldBlockNumOfRecords;
        memcpy(&blockDataFather,blockData + sizeof(char) + sizeof(int), sizeof(int));                                       //Get the father of the old block.
        memcpy(&oldBlockNumOfRecords, blockData + sizeof(char), sizeof(int));
        memcpy(newBlockData+ sizeof(char) + sizeof(int),&blockDataFather, sizeof(int));                        //New block has same father (for now) as old block.
        printf("after initialize\n");

        //Copying half of the values of the old block in the new block..
        int j=0;
        for(int i=oldBlockNumOfRecords/2; i<oldBlockNumOfRecords; i++){
            memcpy(newBlockData + sizeof(char) + 2*sizeof(int) + j*size1 + j*size2, blockData + sizeof(char) + 2*sizeof(int) + i*size1 + i*size2 , size1);
            memcpy(newBlockData + sizeof(char) + 2*sizeof(int) + (j+1)*size1 + j*size2, blockData + sizeof(char) + 2*sizeof(int) + (i+1)*size1 + i*size2 , size2);
            j++;
            //Not deleting old records, but setting the new number of records to the old divided by 2.
        }
        int newBlockNumOfRecords = oldBlockNumOfRecords/2;
        memcpy(blockData + sizeof(char), &newBlockNumOfRecords, sizeof(int));
        //printf("after copy\n");
        //Inserting the values in the new block.
        memcpy(newBlockData + sizeof(char) + 2*sizeof(int) + j* size1 + j* size2,value1,size1);
        memcpy(newBlockData + sizeof(char) + 2*sizeof(int) + j* size1 + j* size2 + size1,value2,size2);
        j++;
        memcpy(newBlockData + sizeof(char),&j, sizeof(int));
        //printf("after insert\n");

        //Passing first value of new block to father.
        void *newvalue = malloc(size1);
        printf("before split newblockdata[0]: %c | newblockdata[1]:%d | newblockdata[5]:%d\n", newBlockData[0],newBlockData[1],newBlockData[5]);
        memcpy(newvalue,newBlockData + sizeof(char) + 2* sizeof(int), size1);
        BF_Block* fatherBlock;
        BF_Block_Init(&fatherBlock);
        printf("before split\n");
        if (BF_GetBlock(fileDesc, blockDataFather, fatherBlock)!=BF_OK) {
            return -1;
        }
        printf("before split\n");
        char* fatherData = BF_Block_GetData(fatherBlock);                                                                        //Gets father Data to pass to Split.
        printf("before split\n");
        SplitBlock(newvalue,0,fatherData,fileDesc,size1,0,type1);                                                               //Calls SplitBlock with the father block data and the key value.
        BF_Block_SetDirty(fatherBlock);
        BF_UnpinBlock(fatherBlock);
    }
    else if (blockData[0] == 'i') {
        printf("In split for index\n");
        int curRecords, blocksNum;
        memcpy(&curRecords,blockData + sizeof(char), sizeof(int));
        int currentBlockSize = sizeof(char) + sizeof(int) + curRecords*sizeof(int) + curRecords*size1;                          //Calculating current block size through the current records in the block.
        if(currentBlockSize + sizeof(int) + size1 <= 512){                                                             //If the block size after the key-sortvalue pair insertion is less than or equal to 512, then insert it
            BF_GetBlockCounter(fileDesc, &blocksNum);
            blocksNum--;
            memcpy(blockData + sizeof(char) + 2*sizeof(int) + curRecords * size1 + curRecords * sizeof(int),&blocksNum, sizeof(int));                   //Insert the key-value pair at the end of the block.
            memcpy(blockData + sizeof(char) + 2*sizeof(int) + (curRecords+1) * size1 + curRecords * sizeof(int),value1, size1);
            curRecords++;
            printf("curRecords %d\n", curRecords);
            memcpy(blockData + sizeof(char),&curRecords, sizeof(int));
            printf("curRecords %d\n", curRecords);
            SortBlock(blockData,size1,type1,size2,curRecords);
            printf("AA\n");
        }
        else{
            int blockDataFather,oldBlockNumOfRecords;
            memcpy(&oldBlockNumOfRecords,blockData + sizeof(char), sizeof(int));                                                //Get the current number of records on the block.
            memcpy(&blockDataFather,blockData + sizeof(char) + sizeof(int), sizeof(int));                                       //Get the father of the old block.
            //Create a new Index Block with half key-value pairs
            BF_Block* nextIndexBlock;
            BF_Block_Init(&nextIndexBlock);
            BF_AllocateBlock(fileDesc, nextIndexBlock);
            char *nextIndexBlockData = BF_Block_GetData(nextIndexBlock);
            char index = 'i';
            memcpy(nextIndexBlockData, &index, sizeof(char));
            memcpy(nextIndexBlockData + sizeof(char) + sizeof(int), &blockDataFather, sizeof(int));
            int j = 0;
            int blocksNum;
            BF_GetBlockCounter(fileDesc, &blocksNum);
            blocksNum--;
            //Copying old records to new block.
            for(int k = oldBlockNumOfRecords/2; k<oldBlockNumOfRecords; k++ ){
                memcpy(nextIndexBlockData + sizeof(char) + 2*sizeof(int) + j* sizeof(int) + j*size1, blockData + sizeof(char) + 2*sizeof(int) + k* sizeof(int) + k*size1, sizeof(int));
                memcpy(nextIndexBlockData + sizeof(char) + 2*sizeof(int) + (j+1)* sizeof(int) + j*size1, blockData + sizeof(char) + 2*sizeof(int) + (k+1)* sizeof(int) + k*size1, size1);
                //After each copy, update child to have the new block as father.
                int childPosition = *blockData + sizeof(char) + 2*sizeof(int) + k* sizeof(int) + k*size1;
                BF_Block *childBlock;
                BF_Block_Init(&childBlock);
                if(BF_GetBlock(fileDesc, childPosition, childBlock)!=BF_OK){
                    return -1;
                }
                char *childBlockData = BF_Block_GetData(childBlock);
                //Updating each child of the new block to have it as father.
                memcpy(childBlockData + sizeof(char) + sizeof(int), &blocksNum, sizeof(int));
                j++;
            }
            //Old index block has half the records now.
            int newBlockNumOfRecords = oldBlockNumOfRecords/2;
            memcpy(blockData + sizeof(char), &newBlockNumOfRecords, sizeof(int));
            //Inserting the values in the new block.
            memcpy(nextIndexBlockData + sizeof(char) + 2*sizeof(int) + j* sizeof(int) + j* size1,&blocksNum, sizeof(int));
            memcpy(nextIndexBlockData + sizeof(char) + 2*sizeof(int) + (j+1)* sizeof(int) + j* size1,value1,size1);
            j++;
            memcpy(nextIndexBlockData + sizeof(char),&j, sizeof(int));
            BF_Block_SetDirty(nextIndexBlock);
            BF_UnpinBlock(nextIndexBlock);
        }
    }
    BF_Block_SetDirty(newBlock);
    BF_UnpinBlock(newBlock);
}

//Function used to sort the block.
//CHANGE IN IMPLEMENTATION: blockData is expected to be the pointer to first ACTUAL DATA of the block
//the curRecords is passed as parameter to enable the above
int SortBlock(char *blockData, int size1,char type1, int size2, int curRecords){
    void *sortValue1=malloc(size1);
    void *sortValue2=malloc(size1);
    void *suppValue1=malloc(size2);
    void *suppValue2=malloc(size2);
    //Basically BubbleSort
    //keep in mind that when splitting, the smallest values are always the leftmost
    //no need for different algorithms for index and data, only need to ignore the pointer to first block pointer in index blocks
    for(int j=0; j<curRecords-1;j++){                                                                                               //This for loop gets the first non-sorted block each time.
        memcpy(suppValue1,blockData + j*(size1+size2) + size1, size2);
        memcpy(sortValue1,blockData + j*(size1+size2), size1);
        for(int k=j; k<curRecords-1;k++){
            memcpy(suppValue2,blockData + (k+1)*(size1+size2) + size1, size2);
            memcpy(sortValue2,blockData + (k+1)*(size1+size2), size1);
            if(scanOpCodeHelper(sortValue2,sortValue1,type1)){
                void* tempV2=malloc(size2);
                void* tempV1=malloc(size1);
                memcpy(tempV2, suppValue1, size2);
                memcpy(tempV1, sortValue1, size1);
                //Overwriting new minimum over old minimum
                memcpy(blockData + j*(size1+size2) + size1, suppValue2, size2);
                memcpy(blockData + j*(size1+size2),sortValue2, size1);
                //Overwriting old minimum over new minimum's place.
                memcpy(blockData + (k+1)*(size1+size2) + size1,tempV2, size2);
                memcpy(blockData + (k+1)*(size1+size2),tempV1, size1);
            }
        }
    }
    return 1;
}

int AM_OpenIndexScan(int fileDesc, int op, void *value) {
    printf("SCAN\n");
    //check for valid fileDesc
    if (fileTable[fileDesc].fileName == NULL) {
        return 1; //no open file in this position
    }
    //allocate scan table position and begin searching
    int i=0;
    int scanTablePos = -1;
    BF_Block* curBlock; //block being parsed
    BF_Block_Init(&curBlock);
    char type1, type2; //type of value1 and value2 of selected file respectively
    int size1, size2; //sizes of value1 and value2 of selected file respectively
    char* bData;
    for (;i<20;i++) {
        if (scanTable[i].scanFile == -1) {
            scanTablePos = i;
            scanTable[i].scanFile = fileDesc;
            if (BF_GetBlock(fileDesc, 0, curBlock)!=BF_OK) { //get file info from block 0
                return -1;
            }
            bData = BF_Block_GetData(curBlock);
            memcpy(&type1, bData, sizeof(char)); 
            memcpy(&size1, bData + sizeof(char), sizeof(int));
            memcpy(&type2, bData + sizeof(char) + sizeof(int), sizeof(char));
            memcpy(&size2, bData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));
            scanTable[i].queryValue = value;
            scanTable[i].opcode = op;
            printf("%c %d %c %d\n", type1, size1, type2, size2);
            BF_UnpinBlock(curBlock);
            break;
        }
    }
    if (scanTablePos==-1) { //no space in scan table
        return scanTablePos;
    }
    //we need to store current block to pass its value in scan struct later
    int currentBlock = 2; //master index block is block 2 (check createindex)
    int currentOffset = sizeof(char) + sizeof(int) + sizeof(int); //go past the index/data/father indicator and the total values
    if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
        return -1;
    }
    bData = BF_Block_GetData(curBlock);
    /* explanation for the offsets below:
        Block format is 
        {index/data byte|int with # of delimValues|int to lower level block|sizeof(value1) sorting value|int to lower level block|etc...}
        In index loop we move forward one int, check the value and repeat until we have to move down a level
    */
    printf("%d %d %hhx %d\n", fileDesc, currentBlock, bData[0], bData[1]);
    int valuesSearched=0;
    //TODO INCONSISTENT BLOCK LOADS?!?!!?!?!?!?!?!!
    //could be due to no unpinning when returning result from findnextindex!
    //printf("%d %d %c %d %d\n",i, currentBlock, bData[0], bData[1], bData[5]);
    //finds first element with the value given, or the last with lower value if no elements with that value
    while (bData[0]=='i') { //loops through index blocks
        int totalDelims;
        memcpy(&totalDelims, bData+sizeof(char), sizeof(int));
        int lastPtrOffset = currentOffset; //offset of latest "pointer"
        currentOffset += sizeof(int); //this pointer is now on next value to check
        //move to lower level if we need less/different(where first shown is the smallest), otherwise check normally to find value equal with the value searched
        if ((op==2 || op==3 || op==5) || valuesSearched>=totalDelims || scanOpCodeHelper(value, bData+currentOffset*sizeof(char), type1)) {
            int nextBlock;
            memcpy(&nextBlock, bData+lastPtrOffset, sizeof(int));
            printf("%d\n", nextBlock);
            BF_UnpinBlock(curBlock);
            if (BF_GetBlock(fileDesc, nextBlock, curBlock)!=BF_OK) {
                return -1;
            }
            bData = BF_Block_GetData(curBlock);
            currentOffset = sizeof(char) + sizeof(int) + sizeof(int); //Changed to work with father.
            currentBlock = nextBlock;
            valuesSearched=0;
        }
        else {
            //still on the same block, move to next pointer
            currentOffset += size1;
            valuesSearched++;
        }
        printf("Scanning index...\n");
    }
    if (bData[0]!='d') {
        printf("ERROR: scan ended on non data block.\n");
        return 10;
    }
    printf("In data block...\n");
    if (op==2 || op==3 || op==5) { //it will already be on first data block from the above loop
        scanTable[i].scanNextBlock = currentBlock;
        scanTable[i].scanNextOffset = currentOffset;
        BF_UnpinBlock(curBlock);
        //when exiting from here, currentOffset shows the first value in first block, which can be <, =, > to value
        return i;
    }
    int tempOffset = currentOffset;
    int allBlockEntries;
    memcpy(&allBlockEntries, bData+sizeof(char), sizeof(int));
    while(1) { //moves to a result that ensures >=, > and == will be correct, provided a check in findNextEntry
        if (((currentOffset-sizeof(char)-sizeof(int)- sizeof(int)-sizeof(int))/(size1+size2)+1)<=allBlockEntries) { //the entry we are going to see is a valid entry
            if (scanOpCodeHelper(bData+currentOffset, value, type1)) {
                currentOffset += size1 + size2; //move to next entry if current is less than the searched
            }
            else break;
        }
        else { //no more entries in this block, means the value we store is the first of the next block
            int nextBlock;
            memcpy(&nextBlock, bData + BF_BLOCK_SIZE-sizeof(int), sizeof(int));
            //no need to get data, we only need the block values
            currentBlock = nextBlock;
            currentOffset = sizeof(char) + sizeof(int) + sizeof(int);
            //REMINDER: IF ON LAST DATA BLOCK, WRITE THE NEXTBLOCK VARIABLE SPACE WITH -1, AND CHECK FROM FINDNEXTENTRY FOR THIS
            break;
        }
    }
    BF_UnpinBlock(curBlock);
    //when exiting, currentOffset has one of the following values:
    //-the first entry with value1==value
    //-the first entry with value1>value, either in same or next data block
    scanTable[i].scanNextBlock = currentBlock;
    scanTable[i].scanNextOffset = currentOffset;
    printf("Ended with scan data: %d %d %d %s %d\n", i, scanTable[i].scanNextBlock, scanTable[i].scanNextOffset, scanTable[i].queryValue, scanTable[i].opcode);
    return i;
}

int scanOpCodeHelper(void* value1, void* value2, char type) {
    //returns true when it finds a greater value as delimiter than the one we search for\
    //aka value1 < value2
    void *delimValue;
    switch(type) {
        case 'c':
            if (strcmp(value1, value2) < 0) {
                return 1;
            }
            break;
        case 'i':
            delimValue = malloc(sizeof(int));
            memcpy(delimValue, value2, sizeof(int));
            if ( *(int*)value1 < *(int*)delimValue) {
                return 1;
            }
            break;
        case 'f':
            delimValue = malloc(sizeof(float));
            memcpy(delimValue, value2, sizeof(float));
            if ( *(float*)value1 < *(float*)delimValue) {
                return 1;
            }
            break;
    }
    return 0;
}


void *AM_FindNextEntry(int scanDesc) {
    BF_Block* curBlock; //block being parsed
    BF_Block_Init(&curBlock);
    char type1, type2; //type of value1 and value2 of selected file respectively
    int size1, size2; //sizes of value1 and value2 of selected file respectively
    char* bData;
	if (BF_GetBlock(scanTable[scanDesc].scanFile, 0, curBlock)!=BF_OK) { //get file info from block 0
        return NULL;
    }
    bData = BF_Block_GetData(curBlock);
    memcpy(&type1, bData, sizeof(char)); 
    memcpy(&size1, bData + sizeof(char), sizeof(int));
    memcpy(&type2, bData + sizeof(char) + sizeof(int), sizeof(char));
    memcpy(&size2, bData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));
    //get block parsed for next result
    BF_UnpinBlock(curBlock);
    //scanNextBlock could be -1, meaning no more entries
    if (BF_GetBlock(scanTable[scanDesc].scanFile, scanTable[scanDesc].scanNextBlock, curBlock)!=BF_OK) {
        return NULL;
    }
    bData = BF_Block_GetData(curBlock);
    int allBlockEntries;
    memcpy(&allBlockEntries, bData+sizeof(char), sizeof(int));
    //check if we are in end of current block data
    if (((scanTable[scanDesc].scanNextOffset - sizeof(char) - sizeof(int)- sizeof(int))/(size1+size2)+1)>allBlockEntries) {
        BF_UnpinBlock(curBlock);
        int nextBlock;
        memcpy(&nextBlock, bData + BF_BLOCK_SIZE-sizeof(int), sizeof(int));
        scanTable[scanDesc].scanNextBlock = nextBlock;
        scanTable[scanDesc].scanNextOffset = sizeof(char) + sizeof(int) + sizeof(int); //updated for father.
        //fixing scantable data and recursing for next block
        AM_FindNextEntry(scanDesc);
    }
    //check for EOF
    if (scanTable[scanDesc].scanNextBlock==-1) {
        AM_errno = AME_EOF;
        return NULL;
    }
    if (scanTable[scanDesc].opcode==1) { //=
        //as explained in openScanFile, the offset is initialized as first equal or a bigger element
        //we only need to check if we reached a bigger element
        if (scanOpCodeHelper(scanTable[scanDesc].queryValue, bData+scanTable[scanDesc].scanNextOffset, type1)) {
            AM_errno = AME_EOF;
            BF_UnpinBlock(curBlock);
            return NULL;
        }
        else {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return bData + scanTable[scanDesc].scanNextOffset - size2;
        }
    }
    if (scanTable[scanDesc].opcode==2) { //!=
        //similar to the >, we print all elements and recurse through the equals
        if (scanOpCodeHelper(scanTable[scanDesc].queryValue, bData+scanTable[scanDesc].scanNextOffset, type1) || scanOpCodeHelper(bData+scanTable[scanDesc].scanNextOffset, scanTable[scanDesc].queryValue, type1)) {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return bData + scanTable[scanDesc].scanNextOffset - size2;
        }
        else {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return AM_FindNextEntry(scanDesc); //pray to god this works...
        }
    }
    else if (scanTable[scanDesc].opcode==3) { //<
        if (scanOpCodeHelper(bData+scanTable[scanDesc].scanNextOffset, scanTable[scanDesc].queryValue, type1)) {
            scanTable[scanDesc].scanNextOffset += size1 + size2; //dont have to check for end of block, this is done when entering
            return bData + scanTable[scanDesc].scanNextOffset - size2;
        }
        else {
            AM_errno = AME_EOF;
            BF_UnpinBlock(curBlock);
            return NULL;
        }
    }
    else if (scanTable[scanDesc].opcode==4) { //>
        //the scan opened has either equal or > elements to queryvalue (stated in openScanFile)
        //this makes this request a trivial loop until we pass through the equals to get a greater value
        //meaning, we can also do it recursively, since scanData table is static in memory and not badly affected from recursions
        if (scanOpCodeHelper(scanTable[scanDesc].queryValue, bData+scanTable[scanDesc].scanNextOffset, type1)) {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return bData + scanTable[scanDesc].scanNextOffset - size2;
        }
        else {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return AM_FindNextEntry(scanDesc); //pray to god this works...
        }
    }
    else if (scanTable[scanDesc].opcode==5) { //<=, this is a reverse >
        if (scanOpCodeHelper(scanTable[scanDesc].queryValue, bData+scanTable[scanDesc].scanNextOffset, type1)) {
            AM_errno = AME_EOF;
            BF_UnpinBlock(curBlock);
            return NULL;
        }
        else {
            scanTable[scanDesc].scanNextOffset += size1 + size2;
            return bData + scanTable[scanDesc].scanNextOffset - size2;
        }
    }
    else if (scanTable[scanDesc].opcode==6) { //>=
        //the scan opened has either equal or > elements to queryvalue (stated in openScanFile)
        //eof is tested above
        //all cases are correct
        scanTable[scanDesc].scanNextOffset += size1 + size2;
        return bData + scanTable[scanDesc].scanNextOffset - size2;
    }
}


int AM_CloseIndexScan(int scanDesc) {
    //unpin open block used to return entries
    scanTable[scanDesc].scanFile = -1;
    scanTable[scanDesc].scanNextBlock = -1;
    scanTable[scanDesc].scanNextOffset = -1;
    return AME_OK;
}


void AM_PrintError(char *errString) {
    printf("Error: %s\n", errString);
    printf("Error Number: %d\n", AM_errno);
}

void AM_Close() {

}
