#ifndef AM_H_
#define AM_H_

/* Error codes */

extern int AM_errno;

#define AME_OK 0
#define AME_EOF -1

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6

typedef struct attributeInfo{
    char attributeType;
    int attributeLength;
} fieldInfo;

typedef struct fileData {                               //data structure for file data
  char* fileName;                                       //needed for file delete
} fileData;

typedef struct scanData {
  int scanFile;                                         //the position of the file being scanned on the fileTable
  int scanStartBlock;                                   //the data block index of the first matching record
  int scanStartOffset;                                  //the data block offset (in bytes) of the first matching record
  int scanEndBlock;                                     //the data block index of the last matching record
  int scanEndOffset;                                    //the data block offset (in bytes) of the last matching record
} scanData;

fileData fileTable[20]; //file data table

scanData scanTable[20]; //scan data table

void AM_Init( void );


int AM_CreateIndex(
  char *fileName, /* όνομα αρχείου */
  char attrType1, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength1, /* μήκος πρώτου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
  char attrType2, /* τύπος πρώτου πεδίου: 'c' (συμβολοσειρά), 'i' (ακέραιος), 'f' (πραγματικός) */
  int attrLength2 /* μήκος δεύτερου πεδίου: 4 γιά 'i' ή 'f', 1-255 γιά 'c' */
);


int AM_DestroyIndex(
  char *fileName /* όνομα αρχείου */
);


int AM_OpenIndex (
  char *fileName /* όνομα αρχείου */
);


int AM_CloseIndex (
  int fileDesc /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
);


int AM_InsertEntry(
  int fileDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  void *value1, /* τιμή του πεδίου-κλειδιού προς εισαγωγή */
  void *value2 /* τιμή του δεύτερου πεδίου της εγγραφής προς εισαγωγή */
);


int AM_OpenIndexScan(
  int fileDesc, /* αριθμός που αντιστοιχεί στο ανοιχτό αρχείο */
  int op, /* τελεστής σύγκρισης */
  void *value /* τιμή του πεδίου-κλειδιού προς σύγκριση */
);


void *AM_FindNextEntry(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


int AM_CloseIndexScan(
  int scanDesc /* αριθμός που αντιστοιχεί στην ανοιχτή σάρωση */
);


void AM_PrintError(
  char *errString /* κείμενο για εκτύπωση */
);

void AM_Close();


#endif /* AM_H_ */
