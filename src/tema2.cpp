#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>

using namespace std;

struct Myfile {
	string name;
    int pieces;
	vector<string> bits;
    vector<bool> received;
    vector<int> peers;
};

struct Filesystem {
    string name;
    vector<int> peers;
    int pieces;
    vector<string> bits;
};

struct down {
    int rank;
    vector<Myfile> *wanted;
};

struct up {
    int rank;
    vector<Myfile> owned;
    int nr_sent;
};

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

void *download_thread_func(void *arg)
{
    struct down* data = (struct down*) arg;
    int count = 0;
    char message[4];

    // iau informatiile despre fiecare fisier dorit de la tracker
    for(int i = 0; i < data->wanted->size(); i++) {
        MPI_Send(data->wanted->at(i).name.c_str(), data->wanted->at(i).name.size() + 1, MPI_CHAR, TRACKER_RANK, 4, MPI_COMM_WORLD);

        int pieces;
        MPI_Recv(&pieces, 1, MPI_INT, TRACKER_RANK, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        data->wanted->at(i).pieces = pieces;

        for(int j = 0; j < data->wanted->at(i).pieces; j++) {

            char hash[HASH_SIZE];
            MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            data->wanted->at(i).bits.push_back(hash);
        }
        data->wanted->at(i).received.assign(data->wanted->at(i).pieces, false);

        MPI_Send(data->wanted->at(i).name.c_str(), data->wanted->at(i).name.size() + 1, MPI_CHAR, TRACKER_RANK, 44, MPI_COMM_WORLD);

        int size;
        MPI_Recv(&size, 1, MPI_INT, TRACKER_RANK, 44, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for(int j = 0; j < size; j++) {

            int client;
            MPI_Recv(&client, 1, MPI_INT, TRACKER_RANK, 44, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            data->wanted->at(i).peers.push_back(client);
        }
    }

    for(int i = 0; i < data->wanted->size(); i++) {

        for(int j = 0; j < data->wanted->at(i).pieces; j++) {

            vector<pair<int,int>> clients;
            // cer de la fiecare client cate segmente a trimis pana acum
            for(int k = 0; k < data->wanted->at(i).peers.size(); k++) {

                int dest = data->wanted->at(i).peers[k];
                MPI_Send(message, 4, MPI_CHAR, dest, 8, MPI_COMM_WORLD);
                int nr;
                MPI_Recv(&nr, 1, MPI_INT, dest, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                clients.push_back({dest, nr});
            }

            // sortez crescator pentru a il alege pe cel mai putin incarcat
            sort(clients.begin(), clients.end(), [](const pair<int, int>& a, const pair<int, int>& b) {
                return a.second < b.second;
            });

            // incerc sa fac rost de segmentul dorit in ordine de la cei mai liberi peers
            int p = 0, ok = 0;
            do {
                MPI_Send(data->wanted->at(i).bits[j].c_str(), HASH_SIZE + 1, MPI_CHAR, clients[p].first, 6, MPI_COMM_WORLD);
                MPI_Recv(message, 4, MPI_CHAR, clients[p].first, 7, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                if(strcmp(message,"ACK") == 0) {

                    data->wanted->at(i).received[j] = true;
                    count++;
                    ok = 1;
                }
                p++;

            }while(p < clients.size() && ok == 0);

            // la fiecare 10 descarcari actualizez lista de peers
            if(count%10 == 0) {

                MPI_Send(data->wanted->at(i).name.c_str(), data->wanted->at(i).name.size() + 1, MPI_CHAR, TRACKER_RANK, 44, MPI_COMM_WORLD);
                int size;
                MPI_Recv(&size, 1, MPI_INT, TRACKER_RANK, 44, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for(int j = 0; j < size; j++) {

                    int client;
                    MPI_Recv(&client, 1, MPI_INT, TRACKER_RANK, 44, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    data->wanted->at(i).peers.push_back(client);
                }
            }
        }
    }

    // semnal de terminare de descarcare
    int final = 99;
    MPI_Send(&final, 1, MPI_INT, TRACKER_RANK, 99, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    struct up* data = (struct up*) arg;
    
    bool running = true;
    while (running) {

        char buffer[100];
        MPI_Status status;

        // primesc orice mesaj si verific daca e pentru mine cu probe
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        int source = status.MPI_SOURCE;
        int tag = status.MPI_TAG;

        if(tag == 100) {

            //mesaj de oprire
            MPI_Recv(buffer, 100, MPI_CHAR, TRACKER_RANK, 100, MPI_COMM_WORLD, &status);
            running = false;
        }
        else if(tag == 6) {

            //mesaj de download al unui segment
            MPI_Recv(buffer, 100, MPI_CHAR, source, 6, MPI_COMM_WORLD, &status);

            bool index = false;
            for(int i = 0; i < data->owned.size(); i++)
                for(int j = 0; j < data->owned[i].pieces; j++)
                    if(data->owned[i].bits[j] == buffer)
                        index = true;

            if(index == true) {
                char message[4] = "ACK";
                MPI_Send(message, 4, MPI_CHAR, source, 7, MPI_COMM_WORLD);
                data->nr_sent++;
            }
            else {
                char message[4] = "NOP";
                MPI_Send(message, 4, MPI_CHAR, source, 7, MPI_COMM_WORLD);
            }
        }

        else if(tag == 8) {
            // trimite clientului sursa cate segmente a mai trimis deja
            MPI_Recv(buffer, 100, MPI_CHAR, source, 8, MPI_COMM_WORLD, &status);
            MPI_Send(&data->nr_sent, 1, MPI_INT, source, 9, MPI_COMM_WORLD);
        }


    }

    return NULL;
}

void tracker(int numtasks, int rank) {

    vector<Filesystem> files(MAX_FILES);

    int c = 0;

    // primeste de la fiecare client fisierele pentru care sunt seed
    for(int i = 1; i < numtasks; i++) {
        int n;
        MPI_Recv(&n, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < n; j++) {
            char name[MAX_FILENAME];
            int pieces;

            MPI_Recv(name, MAX_FILENAME+1, MPI_CHAR, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // verific sa nu adaug un fisier de mai multe ori
            int index = -1;
            for(int l = 0; l < files.size(); l++)
                if(files[l].name == name)
                    index = l;

            if(index==-1) {
                MPI_Recv(&pieces, 1, MPI_INT, i, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                files[c].name = name;
                files[c].pieces = pieces;
                files[c].peers.push_back(i);

                for(int k = 0; k < files[c].pieces; k++) {
                    char hash[HASH_SIZE];
                    MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, i, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    files[c].bits.push_back(hash);
                }

                c++;
            }
            else {
                // daca exista fisierul doar adaug clientul ca seed si nu fac nimic
                files[index].peers.push_back(i);
                MPI_Recv(&pieces, 1, MPI_INT, i, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for(int k = 0; k < files[index].pieces; k++) {
                    char hash[HASH_SIZE];
                    MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, i, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
            }
        }
    }

    // mesaj de incepere descarcare
    char message[4] = "ACK";
    MPI_Bcast(message, 4, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);

    int count = 0;
    bool running = true;

    while(running) {

        char buffer[100];
        MPI_Status status;

        MPI_Recv(buffer, 100, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        int source = status.MPI_SOURCE;
        int tag = status.MPI_TAG;

        if(tag == 4) {
            
            // trimit informatii despre un fisier
            int index;
            for(int i = 0; i < files.size(); i++)
                if(files[i].name == buffer)
                    index = i;

            files[index].peers.push_back(source);
            
            MPI_Send(&files[index].pieces, 1, MPI_INT, source, 4, MPI_COMM_WORLD);

            for(int j = 0; j < files[index].pieces; j++)
               MPI_Send(files[index].bits[j].c_str(), HASH_SIZE + 1, MPI_CHAR, source, 4, MPI_COMM_WORLD);
        }

        else if(tag == 44) {

            // trimit lista de seeds/peers
            int index;
            for(int i = 0; i < files.size(); i++)
                if(files[i].name == buffer)
                    index = i;

            int size = files[index].peers.size();
            MPI_Send(&size, 1, MPI_INT, source, 44, MPI_COMM_WORLD);

            for(int j = 0; j < files[index].peers.size(); j++)
                MPI_Send(&files[index].peers[j], 1, MPI_INT, source, 44, MPI_COMM_WORLD);
        }

        else if(tag == 99) {

            // un client a terminat de descarcat
            count++;
            if (count == numtasks-1)
                running = false;
        }
    }

    // mesaj pentru oprirea clientilor
    for(int i = 1; i< numtasks; i++)
        MPI_Send(message, 4, MPI_CHAR, i, 100, MPI_COMM_WORLD);

}

void peer(int numtasks, int rank) {

    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    string fisier = "in" + to_string(rank) + ".txt";
    ifstream f(fisier);

    int n, m;

    f >> n;
    vector<Myfile> owned(n);
    for(int i = 0; i < n; i++) {
        string name;
        f >> owned[i].name >> owned[i].pieces;
        for(int j = 0; j < owned[i].pieces; j++) {
            string bucati;
            f >> bucati;
            owned[i].bits.push_back(bucati);
        }
    }

    f >> m;
    vector<Myfile> wanted(m);

    for(int i = 0; i < m; i++)
        f >> wanted[i].name;

    f.close();

    MPI_Send(&n, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

    // fiecare client trimite la tracker pentru ce fisiere este seed
    for (const auto& file : owned) {
        MPI_Send(file.name.c_str(), file.name.size() + 1, MPI_CHAR, TRACKER_RANK, 1, MPI_COMM_WORLD);

        MPI_Send(&file.pieces, 1, MPI_INT, TRACKER_RANK, 2, MPI_COMM_WORLD);

        for(int j = 0; j < file.pieces; j++)
            MPI_Send(file.bits[j].c_str(), HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, 3, MPI_COMM_WORLD);
    }

    // asteptp broadcast de la tracker pentru a incepe descarcarea
    char message[4];
    MPI_Bcast(message, 4, MPI_CHAR, TRACKER_RANK, MPI_COMM_WORLD);

    struct down download;
    download.rank=rank;
    download.wanted=&wanted;

    struct up upload;
    upload.rank=rank;
    upload.owned=owned;
    upload.nr_sent = 0;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &download);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &upload);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }

    for(int i = 0; i < wanted.size(); i++) {
        string fisierout = "client" + to_string(rank) + "_" + wanted[i].name;
        ofstream file(fisierout);

        for(int j = 0; j < wanted[i].pieces; j++)
            file << wanted[i].bits[j] << endl;
            
        file.close();
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
