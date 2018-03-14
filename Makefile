INC = -I/usr/local/include
LIB = -L/user/local/lib

all: runtest.c util.c asgn2-pthread.c
	gcc $^ -o runtest $(INC) $(LIB) -lcunit -pthread -fopenmp -static -std=c99 -O3
clean:
	rm -rf runtest
