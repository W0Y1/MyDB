#ifndef  _HELPER_H_
#define _HELPER_H_

#include<io.h>
#include<string>
#include<io.h>
#include<vector>
#include<direct.h>
#include<iostream>
using namespace std;


void getAllFiles(string path, vector<string>& files);
void listFiles(string dir);
void getAllFilesName(string path, vector<string>& files);

#endif



