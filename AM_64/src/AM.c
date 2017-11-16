#include "AM.h"
#include "bf.h"
#include "defn.h"
#include <stdio.h>
#include <string.h>

int AM_errno = AME_OK;

void AM_Init() {
    //make sure every global data structure is initialized empty
    int i=0;
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
    if(BF_CreateFile(fileName) == BF_OK){
        if(BF_OpenFile(fileName,&fileDesc) == BF_OK) {
            BF_Block_Init(&block);
            BF_AllocateBlock(fileDesc, block);
            char data = 'd', index = 'i';
            int curBlockSize = 0;

            BF_Block_Init(&datablock0);
            BF_AllocateBlock(fileDesc, datablock0);
            char *initblockData = BF_Block_GetData(datablock0);
            memcpy(initblockData, &data, sizeof(char));
            memcpy(initblockData + sizeof(char), &curBlockSize, sizeof(int));                           //Incremental value for records in block

            BF_Block_Init(&indexblock0);
            BF_AllocateBlock(fileDesc, indexblock0);
            char *initblockIndexData = BF_Block_GetData(indexblock0);
            memcpy(initblockIndexData, &index, sizeof(char));
            memcpy(initblockIndexData + sizeof(char), &curBlockSize, sizeof(int));                      //Incremental value for records in block
            memcpy(initblockIndexData + sizeof(char), &curBlockSize, sizeof(int));                      //Incremental value for records in block


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
                                                                        //(which means there is no open file that's taking that spot.) If it is, make the index equal to i.
            if(fileTable[i].fileIndex == -1){
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
            int size1,size2,currentBlock = 2;                                           //Top index block is block #2
            int currentOffset = sizeof(char);                                           //Every block has 1 header byte
            if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
                return -1;
            }
            char *indexData = BF_Block_GetData(curBlock);
            currentBlock = 0;                                                           //The block with the type definition and length is block #0.
            if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
                return -1;
            }
            char *typeData = BF_Block_GetData(curBlock);
            memcpy(&type1, typeData, sizeof(char));
            memcpy(&size1, typeData + sizeof(char), sizeof(int));
            memcpy(&type2, typeData + sizeof(char) + sizeof(int), sizeof(char));
            memcpy(&size2, typeData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));

            /* Really temporary solution, TBD */
            switch(type1){
                case 'c':
                    char varType;
                    break;
                case 'i':
                    int varType;
                    break;
                case 'f':
                    float varType;
                    break;
            }
            /*End of temporary solution*/

            while (indexData[0]=='i'){
                int maxRecords;
                int sizeOfSortValueToPass = 0;
                memcpy(&maxRecords,indexData + sizeof(char), sizeof(int));
                for(int j=0; j<maxRecords;j++) {
                    sizeOfSortValueToPass += sizeof(char) + sizeof(int) + (j+1)*sizeof(int); //Check Index Block Definition
                    void sortValue;
                    memcpy(&sortValue, indexData + sizeOfSortValueToPass, sizeof(varType));
                    if(scanOpCodeHelper(value1, &sortvalue, type1)){
                        //TODO - Move down a level.
                    }
                    else{
                        //Move to next sort value.
                        sizeOfSortValueToPass += sizeof(varType);
                    }
                }
            }
        }
    }
    return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
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
            scanTable.scanFile = fileDesc;
            if (BF_GetBlock(fileDesc, 0, curBlock)!=BF_OK) { //get file info from block 0
                return -1;
            }
            bData = BF_Block_GetData(curBlock);
            memcpy(&type1, bData, sizeof(char)); 
            memcpy(&size1, bData + sizeof(char), sizeof(int));
            memcpy(&type2, bData + sizeof(char) + sizeof(int), sizeof(char));
            memcpy(&size2, bData + sizeof(char) + sizeof(int) + sizeof(char), sizeof(int));
            scanTable[i].size1 = size1;
            scanTable[i].size2 = size2;
            scanTable[i].queryValue = value;
            scanTable[i].opcode = op;
            BF_UnpinBlock(curBlock);
            break;
        }
    }
    if (scanTablePos==-1) { //no space in scan table
        return scanTablePos;
    }
    //we need to store current block to pass its value in scan struct later
    int currentBlock = 2; //master index block is block 2 (check createindex)
    int currentOffset = sizeof(char) + sizeof(int); //go past the index/data indicator and the total values
    if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
        return -1;
    }
    bData = BF_Block_GetData(curBlock);
    /* explanation for the offsets below:
        Block format is 
        {index/data byte|int with # of delimValues|int to lower level block|sizeof(value1) sorting value|int to lower level block|etc...}
        In index loop we move forward one int, check the value and repeat until we have to move down a level
    */
    int valuesSearched=0;
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
            BF_UnpinBlock(curBlock);
            if (BF_GetBlock(fileDesc, nextBlock, curBlock)!=BF_OK) {
                return -1;
            }
            bData = BF_Block_GetData(curBlock);
            currentOffset = sizeof(char) + sizeof(int);
            currentBlock = nextBlock;
            valuesSearched=0;
        }
        else {
            //still on the same block, move to next pointer
            currentOffset += size1;
            valuesSearched++;
        }
    }
    //TODO add what to do when on data block
    if (bData[0]!='d') {
        printf("ERROR: scan ended on non data block.\n");
        return 10;
    }
    if (op==2 || op==3 || op==5) { //it will already be on first data block from the above loop
        scanTable[i].scanscanNextBlock = currentBlock;
        scanTable[i].scanNextOffset = currentOffset;
        BF_UnpinBlock(curBlock);
        return AME_OK;
    }
    int tempOffset = currentOffset;
    int allBlockEntries;
    memcpy(&allBlockEntries, bData+sizeof(char), sizeof(int));
    while(true) { //moves to a result that ensures >=, > and == will be correct, provided a check in findNextEntry
        if (scanOpCodeHelper(value, bData+currentOffset, type1)) {
            if (((currentOffset-sizeof(char)-sizeof(int))/(size1+size2)+1)<=allBlockEntries) {
                currentOffset += size1 + size2; //move to next entry
            }
            else { //no more entries in this block, means the value we store is the first of the next block
                int nextBlock;
                memcpy(&nextBlock, BF_BLOCK_SIZE-sizeof(int), sizeof(int));
                //no need to get data, we only need the block values
                currentBlock = nextBlock;
                currentOffset = sizeof(char) + sizeof(int);
                //REMINDER: IF ON LAST DATA BLOCK, WRITE THE NEXTBLOCK VARIABLE SPACE WITH -1, AND CHECK FROM FINDNEXTENTRY FOR THIS
                break;
            }
        }
        else break;
    }
    BF_UnpinBlock(curBlock);
    //when exiting, currentOffset has one of the following values:
    //-the first entry with value1==value
    //-the first entry with value1>value, either in same or next data block
    scanTable[i].scanNextBlock = currentBlock;
    scanTable[i].scanNextOffset = currentOffset;
    return AME_OK;
}

bool scanOpCodeHelper(void* value1, void* value2, char type) {
    //returns true when it finds a greater value as delimiter than the one we search for
    //TODO -- NOT READY
    //Function may not always want to return true when it finds greater value
    //I.e. when <=, we want to start from first data block UP TO the one containing our number
    //Need to discuss implementation for this
    switch(type) {
        case 'c':
            if (strcmp(value1, value2) < 0) {
                return true;
            }
            break;
        case 'i':
            int delimValue;
            memcpy(&delimValue, value2, sizeof(int));
            if (*value < delimValue) {
                return true;
            }
            break;
        case 'f':
            float delimValue;
            memcpy(&delimValue, value2, sizeof(float));
            if (*value < delimValue) {
                return true;
            }
            break;
    }
    return false;
}


void *AM_FindNextEntry(int scanDesc) {
	
}


int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}


void AM_PrintError(char *errString) {
  
}

void AM_Close() {
  
}
