#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>

//typedefs
#define COLUMN_USER_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id; //row id
    char user[COLUMN_USER_SIZE + 1];//account for null terminator
    char email[COLUMN_EMAIL_SIZE + 1];//account for null terminator
} Row;

//STRUCT OFFSETS FOR THE ROW STRUCT
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, user);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

//PAGE TABLE
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;



//struct to define an input buffer
typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

//enum for metacommand res
typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

//statement type enum
typedef enum { 
    STATEMENT_INSERT, 
    STATEMENT_SELECT 
} StatementType;



//struct for statements
typedef struct {
  StatementType type;
  Row row_to_insert;
} Statement;

typedef enum { 
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT 
} PrepareResult;

typedef enum { 
    EXECUTE_SUCCESS, 
    EXECUTE_TABLE_FULL 
} ExecuteResult;

typedef struct {
    int fd;
    uint32_t file_len;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager* pages;
} Table;



//serialize a row
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->user), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}
//deserialize a row
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->user), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

//get the page. Accounts for cache misses
void* get_page(Pager* pager, uint32_t page_num){
    if (page_num > TABLE_MAX_PAGES){
        printf("Page number out of bounds: %d\n", page_num);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL){
        //cache missed
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_len / PAGE_SIZE;

        //might have partial page
        if (pager->file_len % PAGE_SIZE){
            num_pages += 1;
        }

        if (page_num <= num_pages){
            lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t read_bytes = read(pager->fd, page,PAGE_SIZE);
            if (read_bytes == -1){ //errored
                printf("error reading ");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

//caluclate how to read particular row to/from the page table
void* row_slot(Table* table, uint32_t row_num) {
    //get the page that the row is on
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    
    void* page = get_page(table->pages, page_num);
    //get location in page
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

//initializes a pager
Pager* pager_open(const char* filename){
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd == -1){//error opening
        printf("Couldn't open file");
        exit(EXIT_FAILURE);
    }

    off_t file_len = lseek(fd,0,SEEK_END);

    Pager* pager = malloc(sizeof(Pager));

    pager->fd = fd;
    pager->file_len = file_len;

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

Table* database_opener(const char* filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager-> file_len / ROW_SIZE; //get the number of rows needed

    //allocate table struct
    Table* table = (Table*)malloc(sizeof(Table));
    table->pages = pager;
    table->num_rows = num_rows;
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages->pages[i] = NULL;
    }
    return table;
}

//free table struct
// void free_table(Table* table) {
//     for (int i = 0; table->pages[i]; i++) {
// 	    free(table->pages[i]);
//     }
//     free(table);
// }

//allocates new memory for an input buffer
InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}


void pager_flush(Pager* pager, uint32_t page_num, uint32_t size){
    if (pager->pages[page_num] == NULL){
        printf("Tried flushing NULL page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1){
        printf("Error seeking\n");
        exit(EXIT_FAILURE);
    }

    ssize_t write_num = write(pager->fd, pager->pages[page_num], size);

    if (write_num == -1){
        printf("Error writing\n");
        exit(EXIT_FAILURE);
    }
}

//close the db by 1) flush cache to disk 2) close db file 3) free memory
void db_close(Table* table){
    Pager* pager = table->pages;
    uint32_t num_full = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full; i++){
        if (pager->pages[i] == NULL){
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    //write the partial pages at the end of the page table
    //take out once B-tree in use
    uint32_t addtl_rows = table->num_rows % ROWS_PER_PAGE;
    if (addtl_rows > 0){
        uint32_t page_num = num_full;
        if(pager->pages[page_num] != NULL){
            pager_flush(pager,page_num, addtl_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int res = close(pager->fd);
    if (res == -1){
        printf("Error closing the file\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        void* page = pager->pages[i];

        if (page){
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

//SQLite prints "db > " to terminal for accepting input
void print_prompt() { 
    printf("db > "); 
}

void read_input(InputBuffer* input_buffer) {
    //read from stdin
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    //if there was no input, error
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

//frees memory
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

//print row to stdout
void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->user, row->email);
}

//execute a metacommand
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

//check that data being inserted is valid
PrepareResult prepare_insert (InputBuffer* input, Statement* statement){
    //read in from command line, check the sizes, then save

    //set statement type
    statement->type = STATEMENT_INSERT;

    //read using strtok to delimit by space
    char* tokens = strtok(input->buffer, " ");
    char* id = strtok(NULL, " ");
    char* user = strtok (NULL, " ");
    char* email = strtok(NULL, " ");

    //check that none are null
    if (id == NULL || user == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    //copy the id into an int
    int id_int = atoi(id);
    
    if (id_int < 0){
        return PREPARE_NEGATIVE_ID;
    }
    //check user and email lengths
    if (strlen(user) > COLUMN_USER_SIZE || strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    //save the data
    statement->row_to_insert.id = id_int;
    strcpy(statement->row_to_insert.user, user);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

//essentially our SQL "compiler"
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        //just call the prepare_insert function
        return prepare_insert(input_buffer, statement);

    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

//execute an insert
ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row* row_inserted = &(statement->row_to_insert);

    serialize_row(row_inserted, row_slot(table, table->num_rows)); //serialize the row into table
    table->num_rows += 1; //increment rows in table

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    //return all rows
    for (uint32_t i = 0; i < table->num_rows; i++){
        //deserialize row for printing
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}
//executes statements
ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]) {

    if (argc < 2){
        printf("Must supply a db filename\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = database_opener(filename);
    //Table* table = new_table();
    //create new InputBuffer struct
    InputBuffer* input_buffer = new_input_buffer();

    //loop until exit command
    while (true) {
        print_prompt();
        read_input(input_buffer); //read the input from the command line

        //if we got ".EXIT" then exit the loop and program
        // if (strcmp(input_buffer->buffer, ".exit") == 0) {
        //     close_input_buffer(input_buffer); //close the buffer
        //     exit(EXIT_SUCCESS);
        // } else {
        //     //otherwise the command is not recognized
        //     printf("Unrecognized command '%s'.\n", input_buffer->buffer);
        // }

        //test input for metacommands first
        if (input_buffer->buffer[0] == '.'){
            switch(do_meta_command(input_buffer, table)){
                //if the metacommand exec was good, then continue
                case (META_COMMAND_SUCCESS):
                    continue;
                // otherwise print error
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        //prepare the statement
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("SYNTAX ERROR. Could not parse '%s'.\n", input_buffer->buffer);
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("Either email or username strings are too long.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword as start of '%s'\n", input_buffer->buffer);
                continue;
        }

        //now do the statement execution
        //execute_statement(&statement);
        switch (execute_statement(&statement, table)){
            case (EXECUTE_SUCCESS):
                printf("Executed properly.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("ERROR: page table full\n");
                break;
        }

    }
}