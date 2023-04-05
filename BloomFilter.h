#pragma once

#include"BitSet.h"
#include<string>

using namespace std;

struct StrHashArv
{
	int operator()(const string& Str)
	{
		int Sum = 0;
		for (int i = 0; i < Str.size(); i++)
		{
			Sum += ((i + 1) * Str[i]);
		}
		return Sum;
	}
};

struct StrHashSum
{
	int operator()(const string& Str)
	{
		int Sum = 0;
		for (int i = 0; i < Str.size(); i++)
		{
			Sum += Str[i];
		}
		return Sum;
	}
};

struct StrHashBKDR
{
	uint64_t operator()(const string& Str)
	{
		uint64_t Sum = 0;
		for (auto ch : Str)
		{
			Sum += ch;
			Sum *= 131;
		}
		return Sum;
	}
};

class BloomFilter
{
public:
	BloomFilter(int size)
		: _size(size / 32 + 1), _bitSets(size) {

	}
	void Set(const string& k)
	{
		size_t PosHash1 = StrHashArv()(k) % _size;//��������,�ַ���ת����
		size_t PosHash2 = StrHashBKDR()(k) % _size;
		size_t PosHash3 = StrHashSum()(k) % _size;
		_bitSets.Set(PosHash1);
		_bitSets.Set(PosHash2);
		_bitSets.Set(PosHash3);
	}

	bool Find(const string& k)//true�ڣ�false���ڡ�ֻҪ��һ��ӳ�䲻���ڣ���һ������
	{
		size_t PosHash1 = StrHashArv()(k) % _size;//��������,�ַ���ת����
		if (_bitSets.Find(PosHash1) == false)
		{
			return false;
		}
		size_t PosHash2 = StrHashBKDR()(k) % _size;
		if (_bitSets.Find(PosHash2) == false)
		{
			return false;
		}
		size_t PosHash3 = StrHashSum()(k) % _size;
		if (_bitSets.Find(PosHash3) == false)
		{
			return false;
		}
		return true;//����λ���ڣ�˵�������ڡ���Ȼ���ڳ�ͻ
	}

	void Init(vector<int>data) {
		for (int i = 0; i < data.size(); i++) {
			_bitSets._bits[i] = data[i];
		}
	}

	vector<int> ReturnData() { return _bitSets._bits; }
private:
	int _size;
	BitSet _bitSets;
};
