#include "AM.h"

//For clarity purposes
#define 'I' INDEX
#define 'D' DATA

int AM_errno = AME_OK;

void AM_Init() {
	return;
}


int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {

    int fileDesc;
    Block *block;
    fieldInfo field1, field2;                               //Storing attribute type and length in the struct fieldInfo. (fieldInfo Declaration is in AM.h)
    if(BF_CreateFile(fileName) == BF_OK){
//        if(BF_OpenFile(filename,&fileDesc) == BF_OK) {
//            BF_Block_Init(&block);
//            BF_AllocateBlock(fileDesc, block);
//
//        } else {
//            BF_PrintError(BF_OpenFile(filename,&fileDesc));
//            return BF_ERROR;
//        }
        field1.attributeType = attrType1;
        field1.attributeLength = attrLength1;
        field2.attributeType = attrType2;
        field2.attributeLength = attrLength2;
    } else {
        BF_PrintError(BF_CreateFile(fileName));
        return BF_ERROR;
    }
    return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
  return AME_OK;
}


int AM_OpenIndex (char *fileName) {
  return AME_OK;
}


int AM_CloseIndex (int fileDesc) {
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  return AME_OK;
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
