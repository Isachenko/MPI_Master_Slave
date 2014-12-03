#include <stdio.h>
#include <mpich/mpi.h>
#include <getopt.h>
#include <string.h>
#include <condition_variable>

#define TAG_NEW_TASK 201
#define TAG_FINISH_WORK 202
#define TAG_SLAVE_READY 203

#define COLOR_RESET "\e[m"
#define COLOR_GREEN "\e[32m"
#define COLOR_RED  "\033[22;31m"

#define MASTER_NUM 0

int fakebuf;
int curPose = 0;
int matrixSize = 0;
int partSize = 200;


// return [size, columnNum1, rowNum1_1, value1_1, rowNum1_2, value1_2, ....]
void getNextPart(FILE* matrixFile, int partSize, int* buf, int* count)
{
    if (partSize > matrixSize - curPose) {
        partSize = matrixSize - curPose;
    }
    int curBufPos = -1;
    buf[++curBufPos] = partSize;
    int columnSize;
    for(int i = 0; i < partSize; ++i) {
        buf[++curBufPos] = curPose;
        fscanf(matrixFile, "%d", &columnSize);
        buf[++curBufPos] = columnSize;
        for(int j = 0; j < columnSize; ++j) {
            fscanf(matrixFile, "%d", buf + (++curBufPos));
            fscanf(matrixFile, "%d", buf + (++curBufPos));
        }
        ++curPose;
    }
    *count = curBufPos + 1;
}

int rank, size;
int vectorSize;
int* vector;

void stopAll() {
    //ToDo:
}

FILE* input(int* argc, char ***argv) {
    if (*argc != 4) {
        printf("Set Param: MatrixfileName, VectorFileName, resultFileName\n");
        stopAll();
        throw 101;
    }
    //matrix
    char* matrixFileName = (*argv)[1];
    FILE* matrixFile = fopen(matrixFileName, "r");
    if (matrixFile == NULL) {
        printf("file null");
    }
    fscanf(matrixFile, "%d", &matrixSize);
    printf("MatrixSize: %d\n", matrixSize);
    //vector
    char* vectorFileName = (*argv)[2];
    FILE* vectorFile = fopen(vectorFileName, "r");
    fscanf(vectorFile, "%d", &vectorSize);
    vector = new int[vectorSize];
    for(int i = 0; i < vectorSize; ++i) {
        fscanf(vectorFile, "%d", vector + i);
    }

    if (vectorSize != matrixSize) {
        printf("matrixSize != vectorSize\n");
        stopAll();
        throw 102;
    }


    fclose(vectorFile);

    return matrixFile;
}

void output(char *ansFileName, int* ans)
{
    FILE* ansFile = fopen(ansFileName, "w");
    for(int i = 0; i < vectorSize; ++i) {
        fprintf(ansFile, "%d ", ans[i]);
    }

    fclose(ansFile);
}

void goMaster(int* argc, char ***argv) {
    //Init---------------------------------
    printf(COLOR_GREEN"Master start\n"COLOR_RESET);
    FILE* matrixFile = input(argc, argv);

    double t1, t2;
    t1 = MPI_Wtime();

    //send vector
    MPI_Bcast(&vectorSize, 1, MPI_INT, MASTER_NUM, MPI_COMM_WORLD);
    MPI_Bcast(vector, vectorSize, MPI_INT, MASTER_NUM, MPI_COMM_WORLD);


    //-------------------------------------
    int stopedSlavesCount = 0;
    int* buf = new int[1 + partSize*(1 + matrixSize*2)];
    int count;
    bool firstTime = true;
    MPI_Status status;
    MPI_Request isend_req;
    while (true) {
        if (!firstTime) {
            MPI_Wait(&isend_req, &status);
        }
        firstTime = false;
        getNextPart(matrixFile, partSize, buf, &count);
        MPI_Recv(&fakebuf, 1, MPI_INT, MPI_ANY_SOURCE, TAG_SLAVE_READY, MPI_COMM_WORLD, &status);
        int slaveNum = status.MPI_SOURCE;
        if (buf[0] != 0) {
            MPI_Isend(buf, count, MPI_INT, slaveNum, TAG_NEW_TASK, MPI_COMM_WORLD, &isend_req); // ISend
        } else {
            buf[0] = 0;
            ++stopedSlavesCount;
            MPI_Isend(buf, 1, MPI_INT, slaveNum, TAG_NEW_TASK, MPI_COMM_WORLD, &isend_req);
        }
        if (stopedSlavesCount == size - 1) {
            break;
        }
    }

    int* fakebuf2 = new int[vectorSize];
    memset(fakebuf2, 0, vectorSize* sizeof(int));
    int* ans = new int[vectorSize];
    memset(ans, 0, vectorSize* sizeof(int));

    MPI_Reduce(fakebuf2, ans, vectorSize, MPI_INT, MPI_SUM, MASTER_NUM, MPI_COMM_WORLD);

    t2 = MPI_Wtime();
    printf( "Elapsed time is %f\n", t2 - t1 );


    char* ansFileName = (*argv)[3];
    output(ansFileName, ans);
    delete buf;
    fclose(matrixFile);
}

void calculate(int* ans, int* buf)
{
    int columnCount = buf[0];
    int curPos = 0;
    for(int i = 0; i < columnCount; ++i) {
        int columnNum = buf[++curPos];
        int elementCount = buf[++curPos];
        for(int j = 0; j < elementCount; ++j) {
            int raw = buf[++curPos];
            int value = buf[++curPos];
            ans[raw] += value * vector[columnNum];
        }
    }
}

int * waitTask()
{
    MPI_Send(&fakebuf, 1, MPI_INT, MASTER_NUM, TAG_SLAVE_READY, MPI_COMM_WORLD);
    int* buf = new int[1 + partSize*(1 + matrixSize*2)];

    MPI_Status status;
    MPI_Probe(MASTER_NUM, TAG_NEW_TASK, MPI_COMM_WORLD, &status);
    int count;
    MPI_Get_count(&status, MPI_INT, &count);
    MPI_Recv(buf, count, MPI_INT, MASTER_NUM, TAG_NEW_TASK, MPI_COMM_WORLD, &status);

    return buf;
}

int * slaveInit()
{
    MPI_Bcast(&vectorSize, 1, MPI_INT, MASTER_NUM, MPI_COMM_WORLD);
    matrixSize = vectorSize;
    vector = new int[vectorSize];
    int* ans = new int[vectorSize];
    memset(ans, 0, vectorSize* sizeof(int));
    MPI_Bcast(vector, vectorSize, MPI_INT, MASTER_NUM, MPI_COMM_WORLD);

    return ans;
}

void goSlave()
{
    int* ans = slaveInit();


    while(true){
        int* buf = waitTask();

        if (buf[0] == 0) {
            break;
        } else {
            calculate(ans, buf);
        }
    }
    MPI_Reduce(ans, NULL, vectorSize, MPI_INT, MPI_SUM, MASTER_NUM, MPI_COMM_WORLD);
}

int main (int argc, char **argv)
{

    MPI_Init (&argc, &argv);	/* starts MPI */
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);	/* get current process id */
    MPI_Comm_size (MPI_COMM_WORLD, &size);	/* get number of processes */
    if (rank == MASTER_NUM) {
        goMaster(&argc, &argv);
    }
    else {
        goSlave();
    }

    printf(COLOR_RESET"finish\n");


    MPI_Finalize();
    return 0;
}
