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
            BF_Block_SetDirty(datablock0);
            BF_UnpinBlock(datablock0);
            BF_Block_Destroy(&datablock0);

            BF_Block_Init(&indexblock0);
            BF_AllocateBlock(fileDesc, indexblock0);
            char *initblockIndexData = BF_Block_GetData(indexblock0);
            memcpy(initblockIndexData, &index, sizeof(char));
            memcpy(initblockIndexData + sizeof(char), &curBlockSize, sizeof(int));                      //Incremental value for records in block
            int rootHasNoFather = -1;
            memcpy(initblockIndexData + sizeof(char) + sizeof(int), &rootHasNoFather, sizeof(int));
            memcpy(initblockIndexData + sizeof(char) + sizeof(int) + sizeof(int), &firstDataBlock, sizeof(int));
            BF_Block_SetDirty(indexblock0);
            BF_UnpinBlock(indexblock0);
            BF_Block_Destroy(&indexblock0);

            char *blockData = BF_Block_GetData(block);
            memcpy(blockData,&attrType1, sizeof(char));                                                 //Writing attributes in the first block.
            memcpy(blockData + sizeof(char),&attrLength1, sizeof(int));
            memcpy(blockData + sizeof(char) + sizeof(int),&attrType2, sizeof(char));
            memcpy(blockData + sizeof(char) + sizeof(int) + sizeof(char),&attrLength2, sizeof(int));
            //value for finding root
            memcpy(blockData + sizeof(char) + sizeof(int) + sizeof(char) + sizeof(int), &previousBlock, sizeof(int));
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
    for (int i=0;i<20;i++) {
        if(fileName != NULL && fileTable[i].fileName != NULL) {
            if (strcmp(fileTable[i].fileName, fileName) == 0) {
                return 1; //file is open
            }
        }
    }
    remove(fileName); //stdio.h function to remove file
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
                    printf("Block Records: %d | Block Father: %d | Block Number: %d |\n", blockData[1],blockData[5],j);
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
                    printf("Block Records: %d | Block Father: %d | Block Number: %d\n", blockData[1],blockData[5],j);
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
            memcpy(&currentBlock, typeData + sizeof(char) + sizeof(int) + sizeof(char) + sizeof(int), sizeof(int));
            BF_UnpinBlock(curBlock);

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
                    sizeOfSortValueToPass = sizeof(char) + 2*sizeof(int) + (j+1)*sizeof(int) + j*size1;//changed from j+1 to j+2                           //Check Index Block Definition
                    void *sortValue;
                    memcpy(sortValue, indexData + sizeOfSortValueToPass, size1);                             //SortValue could be and int,char or float, so just void.
                    //printf("SortValue: %s\n", (char*)sortValue);
                    if(scanOpCodeHelper(value1, sortValue, type1)){                                                                            //scanOpCodeHelper is used to check if sortValue is larger or smaller than the value passed as argument.
                        //printf("In for j < Max Records: %d ||", maxRecords);
                        //Moving down a level, making indexData equal to the new block's index data.
                        //printf("moving down\n");
                        int lowerLevelFileDesc;
                        memcpy(&lowerLevelFileDesc,indexData + sizeof(char) + 2*sizeof(int) + j*(sizeof(int)+size1), sizeof(int));           //lowerLevelFileDesc is used as a pointer to the next level of index blocks.
                        printf("%d\n", lowerLevelFileDesc);
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
                    memcpy(&lowerLevelFileDesc,indexData + sizeof(char) + 2*sizeof(int) + j*(sizeof(int)+size1), sizeof(int));//changed from j to j+1                //lowerLevelFileDesc is used as a pointer to the next level of index blocks.
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
                    SortBlock(indexData+sizeof(char)+2*sizeof(int), size1, type1, size2, curRecords);
                    BF_Block_SetDirty(curBlock);
                    BF_UnpinBlock(curBlock);
                }
                else {
                    //printf("overflow\n");
                    SplitBlock(value1, value2, indexData,fileDesc,size1,size2,type1);
                    BF_Block_SetDirty(curBlock);
                    BF_UnpinBlock(curBlock);
                    //TODO - Split Block
                }
                return AME_OK;
            }
        }
    }
    return AME_OK;
}


int SplitBlock(void *value1, void *value2, char* blockData,int fileDesc, int size1, int size2, char type1) {
    BF_Block* newBlock;
    BF_Block_Init(&newBlock);
    BF_AllocateBlock(fileDesc, newBlock);
    int newBlockNum;
    BF_GetBlockCounter(fileDesc, &newBlockNum);
    newBlockNum--;
    char* newBlockData = BF_Block_GetData(newBlock);
    void* passedToParentValue;
    if (blockData[0]=='d') {
        //initialize new block that is also data
        newBlockData[0]='d';
        //update data block chain
        memcpy(newBlockData+BF_BLOCK_SIZE-sizeof(int), blockData+BF_BLOCK_SIZE-sizeof(int), sizeof(int));
        memcpy(blockData+BF_BLOCK_SIZE-sizeof(int), &newBlockNum, sizeof(int));
        //default same parent
        memcpy(newBlockData+sizeof(char)+sizeof(int), blockData+sizeof(char)+sizeof(int), sizeof(int));
        //copy all data and sort, then split between the 2 blocks
        int fullBlockCount;
        memcpy(&fullBlockCount, blockData+sizeof(char), sizeof(int));
        char* totalData = malloc((size1+size2)*(fullBlockCount+1));
        memcpy(totalData, blockData+sizeof(char)+2*sizeof(int), (size1+size2)*fullBlockCount);
        memcpy(totalData+(size1+size2)*fullBlockCount, value1, size1);
        memcpy(totalData+(size1+size2)*fullBlockCount+size1, value2, size2);
        fullBlockCount++;
        SortBlock(totalData, size1, type1, size2, fullBlockCount);
        int firstHalf=fullBlockCount/2, secHalf=fullBlockCount-fullBlockCount/2;
        memcpy(blockData+sizeof(char), &firstHalf, sizeof(int));
        memcpy(blockData+sizeof(char)+2*sizeof(int), totalData, firstHalf*(size1+size2));
        memcpy(newBlockData+sizeof(char), &secHalf, sizeof(int));
        memcpy(newBlockData+sizeof(char)+2*sizeof(int), totalData+firstHalf*(size1+size2), secHalf*(size1+size2));
        passedToParentValue = newBlockData+sizeof(char)+2*sizeof(int);
    }
    else if (blockData[0]=='i') { //to get here, the index block has no space
        //default same parent
        memcpy(newBlockData+sizeof(char)+sizeof(int), blockData+sizeof(char)+sizeof(int), sizeof(int));
        int fullBlockCount;
        memcpy(&fullBlockCount, blockData+sizeof(char), sizeof(int));
        char* totalData = malloc((size1+size2)*(fullBlockCount+1));
        memcpy(totalData, blockData+sizeof(char)+2*sizeof(int)+sizeof(int), (size1+size2)*fullBlockCount);
        memcpy(totalData+(size1+size2)*fullBlockCount, value1, size1);
        memcpy(totalData+(size1+size2)*fullBlockCount+size1, value2, size2);
        fullBlockCount++;
        SortBlock(totalData, size1, type1, size2, fullBlockCount);
        //do not pass the middle value in second block, it will be passed to parent node as delimiter
        int firstHalf=fullBlockCount/2, secHalf=fullBlockCount-fullBlockCount/2-1;
        memcpy(blockData+sizeof(char), &firstHalf, sizeof(int));
        memcpy(blockData+sizeof(char)+2*sizeof(int), totalData, firstHalf*(size1+size2));
        memcpy(newBlockData+sizeof(char), &secHalf, sizeof(int));
        memcpy(newBlockData+sizeof(char)+2*sizeof(int), totalData+firstHalf*(size1+size2)+size1, secHalf*(size1+size2)+size2);
        //update all children of new block to have new parent
        int i=0;
        int childOffset = sizeof(char)+2*sizeof(int);
        for (;i<=secHalf;i++) {
            BF_Block* childBlock;
            int childBIndex;
            BF_Block_Init(&childBlock);
            memcpy(&childBIndex, newBlockData+childOffset, sizeof(int));
            BF_GetBlock(fileDesc, childBIndex, childBlock);
            char* childBData = BF_Block_GetData(childBlock);
            memcpy(childBData+sizeof(char)+sizeof(int), &newBlockNum, sizeof(int));
            BF_Block_SetDirty(childBlock);
            BF_UnpinBlock(childBlock);
            childOffset += sizeof(int)+size1;
        }
        passedToParentValue = totalData + firstHalf*(size1+size2);
    }
    //check if parent is full or -1
    int parentIndex;
    memcpy(&parentIndex, blockData+sizeof(char)+sizeof(int), sizeof(int));
    BF_Block* parentBlock;
    BF_Block_Init(&parentBlock);
    if (parentIndex==-1) { //new root
        BF_AllocateBlock(fileDesc, parentBlock);
        int rootBlockNum;
        BF_GetBlockCounter(fileDesc, &rootBlockNum);
        rootBlockNum--;
        char* parentBlockData = BF_Block_GetData(parentBlock);
        //initialize new root
        parentBlockData[0]='i';
        int totalRootEntries=1, rootParent=-1;
        memcpy(parentBlockData+sizeof(char), &totalRootEntries, sizeof(int));
        memcpy(parentBlockData+sizeof(char)+sizeof(int), &rootParent, sizeof(int));
        //set the 2 splitted blocks to have parent the new root (we know there are only 2 blocks without parent because there is only 1 root)
        memcpy(blockData+sizeof(char)+sizeof(int), &rootBlockNum, sizeof(int));
        memcpy(newBlockData+sizeof(char)+sizeof(int), &rootBlockNum, sizeof(int));
        //find old root number from block0 info
        BF_Block* infoBlock;
        BF_Block_Init(&infoBlock);
        BF_GetBlock(fileDesc, 0, infoBlock);
        char* infoBData = BF_Block_GetData(infoBlock);
        int rootInfo;
        memcpy(&rootInfo, infoBData+2*sizeof(char)+2*sizeof(int), sizeof(int));
        memcpy(parentBlockData+sizeof(char)+2*sizeof(int), &rootInfo, sizeof(int));
        memcpy(parentBlockData+sizeof(char)+3*sizeof(int), passedToParentValue, size1);
        memcpy(parentBlockData+sizeof(char)+3*sizeof(int)+size1, &newBlockNum, sizeof(int));
        //change root info in block0
        memcpy(infoBData+2*sizeof(char)+2*sizeof(int), &rootBlockNum, sizeof(int));
        BF_Block_SetDirty(infoBlock);
        BF_UnpinBlock(infoBlock);
    }
    else {
        BF_GetBlock(fileDesc, parentIndex, parentBlock);
        char* parentBlockData = BF_Block_GetData(parentBlock);
        int parentDataCount;
        memcpy(&parentDataCount, parentBlockData+sizeof(char), sizeof(int));
        if (sizeof(char)+3*sizeof(int)+(parentDataCount+1)*(size1+sizeof(int))<=BF_BLOCK_SIZE) {
            //there is space
            memcpy(parentBlockData+sizeof(char)+3*sizeof(int)+parentDataCount*(size1+sizeof(int)), passedToParentValue, size1);
            memcpy(parentBlockData+sizeof(char)+3*sizeof(int)+parentDataCount*(size1+sizeof(int))+size1, &newBlockNum, sizeof(int));
            parentDataCount++;
            memcpy(parentBlockData+sizeof(char), &parentDataCount, sizeof(int));
            SortBlock(parentBlockData+sizeof(char)+3*sizeof(int), size1, type1, sizeof(int), parentDataCount);
        }
        else {
            SplitBlock(passedToParentValue, &newBlockNum, parentBlockData, fileDesc, size1, sizeof(int), type1);
        }
    }
    BF_Block_SetDirty(parentBlock);
    BF_UnpinBlock(parentBlock);
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
    int currentBlock;
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
            memcpy(&currentBlock, bData + sizeof(char) + sizeof(int) + sizeof(char) + sizeof(int), sizeof(int));
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
    //finds first element with the value given, or the last with lower value if no elements with that value
    while (bData[0]=='i') { //loops through index blocks
        printf("CCC\n");
        int totalDelims;
        memcpy(&totalDelims, bData+sizeof(char), sizeof(int));
        int lastPtrOffset = currentOffset; //offset of latest "pointer"
        currentOffset += sizeof(int); //this pointer is now on next value to check
        //move to lower level if we need less/different(where first shown is the smallest), otherwise check normally to find value equal with the value searched
        if ((op==2 || op==3 || op==5) || valuesSearched>=totalDelims || scanOpCodeHelper(value, bData+currentOffset*sizeof(char), type1)) {
            int nextBlock;
            memcpy(&nextBlock, bData+lastPtrOffset, sizeof(int));
            printf("NB %d\n", nextBlock);
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
    int curSearchedEntry=1;
    while(1) { //moves to a result that ensures >=, > and == will be correct, provided a check in findNextEntry
        if (curSearchedEntry<=allBlockEntries) { //the entry we are going to see is a valid entry
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
        curSearchedEntry++;
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