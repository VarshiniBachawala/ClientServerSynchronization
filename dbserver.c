#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>    /* Internet domain header */
#include "wrapsock.h"
#include "filedata.h"

/* Wrapper signatures */

ssize_t Readline(int fd, void *ptr, size_t maxlen);
void Writen(int fd, void *ptr, size_t nbytes);
ssize_t Readn(int fd, void *ptr, size_t nbytes);

/* Function signatures */

void close_connection(int sock, struct client_info *client, fd_set *allset);
void process_client_request(int sock, struct client_info *client, struct sync_message received_packet, fd_set *allset);
int send_new_file(int sock, struct client_info *client);
void check_sharing(struct client_info *client, int client_slot);
void add_shared(struct client_info *client, char *filename);
void refresh_file_times(struct client_info *client);
void get_file(int sock, struct client_info *client, char *buffer, int length);
void send_file(int sock, char *directory, char *filename);

int set_up(){
	int listenfd;
	struct sockaddr_in servaddr;	
	int yes = 1;

    listenfd = Socket(AF_INET, SOCK_STREAM, 0);

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port        = htons(PORT);

    if((setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int))) == -1) {
        perror("setsockopt");
    }
    Bind(listenfd, (struct sockaddr *) &servaddr, sizeof(servaddr));

    Listen(listenfd, LISTENQ);
	return listenfd;
}

int main(int argc, char **argv) {
    int i, maxi, maxfd, connfd, sockfd;
    int nready;
    ssize_t	n;
    fd_set rset, allset;
    char buffer[CHUNKSIZE];
    
    struct sockaddr_in cliaddr;
	socklen_t clilen;
    clilen = sizeof(cliaddr);
	int listenfd = set_up();
	
    maxfd = listenfd;
    maxi = -1;

    FD_ZERO(&allset);

	//Set the 'listenfd' to be checked for in the set.
    FD_SET(listenfd, &allset);
    
	//Set up the client's information (populate with default values).
	init();

	printf("INFO: Server booted up.\n");

	/* Login message sent by each client */
	struct login_message handshake;
	int handshake_size = sizeof(handshake);

	struct sync_message received_packet;
	int received_packet_size = sizeof(received_packet);

	int client_slot, get_read_size;
	char path[CHUNKSIZE];

    for ( ; ; ) {
		rset = allset;					// make a copy because rset gets altered
		nready = Select(maxfd+1, &rset, NULL, NULL, NULL);
	
    	//Check for any nre connection.
		if (FD_ISSET(listenfd, &rset)) {	
			connfd = Accept(listenfd, (struct sockaddr *) &cliaddr, &clilen);

			//First message has to be a login message. Read it.
			if((n = Readn(connfd, &handshake, handshake_size)) != handshake_size){
				fprintf(stderr, "Fatal: Expecting handshake packet. Client sent malformed data.\n");
				exit(1);
			}

		  	if((client_slot = add_client(handshake)) >= 0){
				clients[client_slot].sock = connfd;
				clients[client_slot].state = SYNC;

				//Populate the flags with default values.
				clients[client_slot].refresh = 0;
				clients[client_slot].sharing = 0;

				strncpy(path, "server_files/", 14);
				strncat(path, handshake.dir, CHUNKSIZE-14);

				mkdir(path, S_IRWXU); //CHMOD 700; creates if it does not exist.
				
				printf("CONNECT: Accepted a new client: %s\n", handshake.userid);

				check_sharing(&clients[client_slot], client_slot);
			}else{
				//"Too many clients" error would have been raised.
				exit(1);
			}
		 
			//Add the client's socket to the descriptor.
			FD_SET(connfd, &allset);
			
			if (connfd > maxfd)
			  maxfd = connfd;	
			if (i > maxi)
			  maxi = i;	
			
			if (--nready <= 0)
			  continue;	/* no more readable descriptors */
		}
	
   	 	//Check the clients for data.
		for (i = 0; i <= maxi; i++) {
	
	    	if ((sockfd = clients[i].sock) < 0) //Not active.
		 		continue;
	   		if (FD_ISSET(sockfd, &rset)) {

				//Client 'clients[i]' has some data.
				if(clients[i].state == SYNC){

					if(clients[i].refresh){ 
						refresh_file_times(&clients[i]);

						//Once refreshed, do not do so until a cycle is complete.
						clients[i].refresh = 0; 
					}

					if ((n = Readn(sockfd, &received_packet, received_packet_size)) <= 0) {
				    	//Client closed connection.
						close_connection(sockfd, &clients[i], &allset);
					}else{
						//A sync packet has been received, process it.
						process_client_request(sockfd, &clients[i], received_packet, &allset);
					}	

				}else if(clients[i].state == GETFILE){				
					if((get_read_size = clients[i].get_filename_size - clients[i].get_filename_readcount) > CHUNKSIZE){
						get_read_size = CHUNKSIZE;
					}

					if ((n = Readn(sockfd, &buffer, get_read_size)) <= 0) {
						//Client closed connection.
						close_connection(sockfd, &clients[i], &allset);
					}else{
						//Write the file out on the server's file system.						
						get_file(sockfd, &clients[i], buffer, n);
					}
				}
			}

			if (--nready <= 0)
			    break;	/* no more readable descriptors */
	    }
	}    
}

void close_connection(int sock, struct client_info *client, fd_set *allset){
	
	Close(sock);
	//Clear the sock from the set of descriptions being checked.
	FD_CLR(sock, allset);

	client->sock = -1;
	client->state = DEADCLIENT;

	printf("DEAD CLIENT: Closed connection on user: %s\n", client->userid);
}


void process_client_request(int sock, struct client_info *client, struct sync_message received_packet, fd_set *allset){

	struct file_info *current_file;
	struct sync_message response_packet;
	int response_packet_size = sizeof(response_packet);
	
	if(strlen(received_packet.filename) == 0){ 
		//Checking for empty files.
		send_new_file(sock, client);
		client->state = SYNC;

	}else{ 
		//A regular sync packet.

		if(client->sharing){ 
			add_shared(client, received_packet.filename);
		}

		if((current_file = check_file(client->files, received_packet.filename)) == NULL){
			//No more files can be accepted.
			fprintf(stderr, "Maximum file limit reached for directory: %s. Non-graceful kill to client: %s\n", client->dirname, client->userid);
			close_connection(sock, client, allset);

		}else{
			//Construct and send client the respective sync_message packet.
			strncpy(response_packet.filename, current_file->filename, MAXNAME);
			response_packet.mtime = (long int)current_file->mtime;
			response_packet.size = current_file->size;

			Writen(sock, &response_packet, response_packet_size);

			if(received_packet.mtime > response_packet.mtime){
				//Client has a more recent file.
				client->state = GETFILE;
				strncpy(client->get_filename, current_file->filename, MAXNAME);
				client->get_filename_readcount = 0;
				client->get_filename_size = received_packet.size;
				client->get_filename_timestamp = received_packet.mtime;

				//Update the client's file_info for this file.
				current_file->size = received_packet.size;
				current_file->mtime = (time_t)received_packet.mtime;
			
				printf("\tTX: GETFILE: %s into directory: %s, from user: %s\n", current_file->filename, client->dirname, client->userid);

			}else if(received_packet.mtime < response_packet.mtime){
				//Server has a more recent file, send it.
				printf("\tTX: SENDFILE: %s from directory: %s, to user: %s\n", current_file->filename, client->dirname, client->userid);
				send_file(sock, client->dirname, current_file->filename);
				printf("\t\tTX: Complete.\n");
 				client->state = SYNC;
			}
		}	
	}					
}

 
int send_new_file(int sock, struct client_info *client){

	char dirpath[CHUNKSIZE];		
	char fullpath[CHUNKSIZE];
	DIR *dir;
	struct dirent *file;
	struct stat st;
	int j, found;
	struct sync_message response_packet;
	int packet_size = sizeof(response_packet);
	struct file_info* current_file;

	//Get the relative path to this client's directory.
	strncpy(dirpath, "server_files/", 14);
	strncat(dirpath, client->dirname, CHUNKSIZE-14);

	if((dir = opendir(dirpath)) == NULL){
			perror("Opening directory: ");
			exit(1);
	}

	while(((file = readdir(dir)) != NULL)){

		found = 1;

		strncpy(fullpath, dirpath, 256);
		strcat(fullpath, "/");
		strncat(fullpath, file->d_name, CHUNKSIZE-strlen(fullpath)); //error?

		if(stat(fullpath, &st) != 0){
			perror("stat");
			exit(1);
		}
		
		//Check if a regular file (Skip dot files and subdirectories).
		if(S_ISREG(st.st_mode)){
			
			for(j=0; j<MAXFILES; j++){

				if(client->files[j].filename[0] == '\0'){
					
					found = 1;
					break;
				} 

				if(strcmp(client->files[j].filename, file->d_name) == 0){
					//Found some file which exists already exists, skip.
					found = 0;
					break;
				}
			}

			if(found){
				//The file 'file-d_name' at this iteration needs to be sent to the client.

				//Generate and send the approriate sync_message with the file information.
				strncpy(response_packet.filename, file->d_name, MAXNAME);
				response_packet.mtime = (long int)st.st_mtime;
				response_packet.size = (int)st.st_size;
				
				Writen(sock, &response_packet, packet_size);

				//Add the associated modified time, size and the filename itself to the client.
				current_file = check_file(client->files, file->d_name);
				current_file->mtime = (time_t)st.st_mtime;
				current_file->size = (int)st.st_size;

				printf("\tNEWFILE TX: %s does not exist on client %s; sending. \n", file->d_name, client->userid);
				send_file(sock, client->dirname, file->d_name);
				printf("\t\tNEWFILE TX: Complete.\n");
				return 1;
			}

		}
		
	}	
		strncpy(response_packet.filename, "", MAXNAME);
		response_packet.mtime = 0;
		response_packet.size = 0;

		Writen(sock, &response_packet, packet_size);	
		client->refresh = 1;
	
	if(closedir(dir) == -1){
			perror("Closing directory: ");
			exit(1);
	}
	return 0;
}

/* Check if the directory being requested to be synchronized by the client
 * 'client' has already been synchronized by some other client.
 */
void check_sharing(struct client_info *client, int client_slot){

	int i;

	for(i=0; i<MAXCLIENTS; i++){
	
		if(clients[i].userid == '\0'){
			//No more active clients beyond. Quit searching further to improve run-time.
			break;
		}

		if(i == client_slot){
			//Omit this client itself.
			continue;
		}

		if(strcmp(clients[i].dirname, client->dirname) == 0){

			printf("\t\tSHARING: Detected directory %s with client: %s \n", clients[i].dirname, clients[i].userid);

			client->sharing = 1;
			break;
		}
	}
}

/* Check if file 'filename' is already present in the directory being
 * synchronized by the client 'client'.
 */
void add_shared(struct client_info *client, char *filename){

	char dirpath[CHUNKSIZE], fullpath[CHUNKSIZE];
	DIR *dir;
	struct dirent *file;
	struct stat st;
	struct file_info *current_file;

	//Get the relative path to this client's directory.
	strncpy(dirpath, "server_files/", 14);
	strncat(dirpath, client->dirname, CHUNKSIZE-14);

	if((dir = opendir(dirpath)) == NULL){
		perror("Opening directory: ");
		exit(1);
	}

	while(((file = readdir(dir)) != NULL)){
				
		//For every file present on the server.
		if(strcmp(file->d_name, filename) == 0){

			//The file 'filename' exists.
			strncpy(fullpath, dirpath, 256);
			strcat(fullpath, "/");
			strncat(fullpath, file->d_name, CHUNKSIZE-strlen(fullpath)); 

			if(stat(fullpath, &st) != 0){
				perror("stat");
				exit(1);
			}

			//Add the associated modified time, size and the filename itself to the client.
			current_file = check_file(client->files, file->d_name);
			current_file->mtime = (time_t)st.st_mtime;
			current_file->size = (int)st.st_size;
			break;
		}
	}

	if(closedir(dir) == -1){
		perror("Closing directory: ");
		exit(1);
	}
}

/* For every file present in the client 'client' file_info array (for every file
 * that been synchronized) check if the file has been updated on the server's
 * file system. 
 */
void refresh_file_times(struct client_info *client){
	int j;
	char dirpath[CHUNKSIZE];
	char fullpath[CHUNKSIZE];
	DIR *dir;
	struct dirent *file;
	struct stat st;

	//Get the relative path to this client's directory.
	strncpy(dirpath, "server_files/", 14);
	strncat(dirpath, client->dirname, CHUNKSIZE-14);
	
	for(j=0; j<MAXFILES; j++){
					
		if(client->files[j].filename[0] == '\0'){
			//Client has no more files, break out to improve run-time.
			break;
		}else{
			//Open the server directory for this clients and check if this file exists.
			if((dir = opendir(dirpath)) == NULL){
				perror("Opening directory: ");
				exit(1);
			}

			while(((file = readdir(dir)) != NULL)){

				if(strcmp(client->files[j].filename, file->d_name) == 0){
					//This file exists on the server (has been synchronized before).

					strncpy(fullpath, dirpath, CHUNKSIZE);
					strcat(fullpath, "/");
					strncat(fullpath, file->d_name, CHUNKSIZE-strlen(file->d_name));

					if(stat(fullpath, &st) != 0){
						perror("stat");
						exit(1);
					}
					
					/* Check if the server's filesystem has a new modiciation time
					 * for this file, if so update this client's file information
					 * array.
					 */
					if(client->files[j].mtime < st.st_mtime){
						printf("\tMODIFIED: On SERVER: %s\n", client->files[j].filename);
						client->files[j].mtime = st.st_mtime;
						client->files[j].size = (int)st.st_size;
					}

					break;
				}

			}

			if(closedir(dir) == -1){
				perror("Closing directory: ");
				exit(1);
			}
		}
	}
}

void get_file(int sock, struct client_info *client, char *buffer, int length){
	
	FILE *fp;
	char fullpath[CHUNKSIZE];	

	//Grab the full path to this file on the server.	
	strncpy(fullpath, "server_files/", 14);
	strncat(fullpath, client->dirname, CHUNKSIZE-14);
	strcat(fullpath, "/");
	strncat(fullpath, client->get_filename, CHUNKSIZE-strlen(fullpath));
	if(client->get_filename_readcount){
		if((fp = fopen(fullpath, "a")) == NULL) {
			perror("fopen on get file: ");
			exit(1);
    	}
	}else{
		if((fp = fopen(fullpath, "w")) == NULL) {
			perror("fopen on get file: ");
			exit(1);
    	}
	}

	//Write the contents present in 'buffer' of length 'length' to the file.
	fwrite(buffer, length, 1, fp);

	//If there was an error with fwrite.
	if(ferror(fp)){
    	fprintf(stderr, "A write error occured.\n");
		Close(sock);
		exit(1);
	}
	client->get_filename_readcount += length;
	if(client->get_filename_readcount == client->get_filename_size){
		printf("\t\tCOMPLETE TX: %s into directory: %s, from user: %s\n", client->get_filename, client->dirname, client->userid);
		client->state = SYNC;
	}	
	
	if((fclose(fp))) {
		perror("fclose: ");
		exit(1);
    }

	struct stat sbuf;
    struct utimbuf new_times;
	
	if(stat(fullpath, &sbuf) != 0) {
    	perror("stat");
    	exit(1);
    }
	new_times.actime = sbuf.st_atime; //Access time.
	new_times.modtime = (time_t)client->get_filename_timestamp;
	
	if(utime(fullpath, &new_times) < 0) {
    	perror("utime");
    	exit(1);
    }
}

void send_file(int sock, char *directory, char *filename){
	
	FILE *fp;
	char fullpath[CHUNKSIZE];	
	char buffer[CHUNKSIZE];
	int bufsize = CHUNKSIZE;
	int i;

	//Grab the full path to the file on the server.
	strncpy(fullpath, "server_files/", 14);
	strncat(fullpath, directory, CHUNKSIZE-14);
	strcat(fullpath, "/");
	strncat(fullpath, filename, CHUNKSIZE-strlen(fullpath));

	if((fp = fopen(fullpath, "r")) == NULL) {
		perror("fopen on send file: ");
		exit(1);
    }

	//Read up to bufsize or EOF (whichever occurs first) from 'fp'.
	while((i = fread(buffer, 1, bufsize, fp))){
		if(ferror(fp)){
    		fprintf(stderr, "A read error occured.\n");
			Close(sock);
			exit(1);
		}

		//Write the 'i' bytes read from the file to the socket 'soc'.	
		Writen(sock, &buffer, i);
	}

	if((fclose(fp))) {
		perror("fclose: ");
		exit(1);
    }
}
