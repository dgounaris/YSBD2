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

            BF_Block_Init(&datablock0);
            BF_AllocateBlock(fileDesc, datablock0);
            char *initblockData = BF_Block_GetData(datablock0);
            memcpy(initblockData, &data, sizeof(char));

            BF_Block_Init(&indexblock0);
            BF_AllocateBlock(fileDesc, indexblock0);
            char *initblockIndexData = BF_Block_GetData(indexblock0);
            memcpy(initblockData, &index, sizeof(char));


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
    if(BF_CloseFile(fileDesc) == BF_OK){                                //Closing File and checking if it has closed successfully.
        for(int i=0; i<20; i++){                                        //For each record in the fileTable array, check if the fileIndex is equal to the fileDesc.
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
  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
    //check for valid fileDesc
    if (fileTable[i].fileName == NULL) {
        return 1; //no open file in this position
    }
    //allocate scan table position and begin searching
    int i=0;
    int scanTablePos = -1;
    BF_Block* curBlock; //block being parsed
    BF_Block_Init(&curBlock);
    char type1, type2; //type of value1 and value2 of selected file respectively
    int size1, size2; //sizes of value1 and value2 of selected file respectively
    char* bData
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
        }
    }
    if (scanTablePos==-1) { //no space in scan table
        return scanTablePos;
    }
    //we need to store current block to pass its value in scan struct later
    int currentBlock = 2; //master index block is block 2 (check createindex)
    int currentOffset = sizeof(char); //all blocks have 1 header byte
    if (BF_GetBlock(fileDesc, currentBlock, curBlock)!=BF_OK) {
        return -1;
    }
    bData = BF_Block_GetData(curBlock);
    /* explanation for the offsets below:
        Block format is 
        {index/data byte|int to lower level block|sizeof(value1) sorting value|int to lower level block|etc...}
        In index loop we move forward one int, check the value and repeat until we have to move down a level
    */
    while (bData[0]=='i') { //loops through index blocks
        int lastPtrOffset = currentOffset; //offset of latest "pointer"
        currentOffset += sizeof(int); //this pointer is now on next value to check
        if (scanOpCodeHelper(value, bData+currentOffset*sizeof(char), op, type1)) {
            //moving down a level
            int nextBlock;
            memcpy(&nextBlock, bData+lastPtrOffset, sizeof(int));
            BF_UnpinBlock(curBlock);
            BF_GetBlock(fileDesc, nextBlock, curBlock);
            bData = BF_Block_GetData(curBlock);
            currentOffset = sizeof(char);
            currentBlock = nextBlock;
        }
        else {
            //still on the same block, move to next pointer
            currentOffset += size1;
        }
    }
    //TODO add what to do when on data block
    return AME_OK;
}

bool scanOpCodeHelper(void* value1, void* value2, int op, char type) {
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
