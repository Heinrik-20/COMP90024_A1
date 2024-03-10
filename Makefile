all: ./build/main

./build/main: ./build/main.o ./build/data.o ./build/time_utils.o
	mpicc -Wall -g -o ./build/main ./build/main.o ./build/data.o ./build/time_utils.o -lm

./build/main.o: main.c
	mpicc -Wall -c -o ./build/main.o main.c -g

./build/data.o: ./utils/data.c ./utils/data.h
	mpicc -Wall -c -o ./build/data.o ./utils/data.c -g

./build/time_utils.o: ./utils/time_utils.c ./utils/time_utils.h
	mpicc -Wall -c -o ./build/time_utils.o ./utils/time_utils.c -g

clean:
	rm -r build && mkdir ./build