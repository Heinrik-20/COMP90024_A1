all: ./build/main

./build/main: ./build/main.o
	mpicc -g -o ./build/main ./build/main.o

./build/main.o: main.c
	mpicc -c -o ./build/main.o main.c

clean:
	rm -r build && mkdir ./build