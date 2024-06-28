CC=g++
CXX=g++
LD=g++

EXESRC=SampleClient.cpp ../Barrier/Barrier.cpp
EXEOBJ=$(EXESRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
LDFLAGS = -L. -lMapReduceFramework

EXE = SampleClient
TARGETS = $(EXE)

TAR=tar
TARFLAGS=-cvf
TARNAME=sampleclient.tar
TARSRCS=$(EXESRC) Makefile

SampleClient.o: SampleClient.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

libMapReduceFramework.a: MapReduceFramework.o
	ar rcs $@ $^

all: $(TARGETS)

$(TARGETS): $(EXEOBJ)
	$(LD) $(CXXFLAGS) $(EXEOBJ) -o $(EXE) $(LDFLAGS)

clean:
	$(RM) $(TARGETS) $(EXE) $(OBJ) $(EXEOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
