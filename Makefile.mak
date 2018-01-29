CC = g++
HADOOP_INSTALL = /home/hadoop/hadoop
INC = -I$(HADOOP_INSTALL)/include
LIBS = -L$(HADOOP_INSTALL)/lib/native -lhadooppipes -lhadooputils -lpthread -lssl -lcrypto
CPPFLAGS = $(INC) -Wall -g -O2

moviesPerGenre: Atul_Joshi_Program_1.cpp
        $(CC) $(CPPFLAGS) -o $@ $< $(LIBS)
