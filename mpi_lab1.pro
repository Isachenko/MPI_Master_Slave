TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += main.cpp

INCLUDEPATH += /usr/include/mpich2/
LIBS += -lmpich -lopa -lpthread -lrt
QMAKE_CXXFLAGS += -Bsymbolic-functions

CONFIG += -std = c++11

HEADERS +=
