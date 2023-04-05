#pragma once
#include<vector>

using namespace std;

class BitSet
{
public:
	friend class BloomFilter;

	BitSet(int size) {
		_bits.resize((size / (4 * 8)) + 1, 0);//�࿪һ����������֤��������������ӳ�䵽
	}
	 
	void Set(size_t x)//��xӳ���λ���Ϊ1
	{
		//�ȼ���x���Ǹ�������
		size_t index_int = x / 32;
		//����x����һλ
		size_t index_bit = x % 32;

		_bits[index_int] |= (1 << index_bit);
	}

	void ReSet(size_t x)//��xӳ���λ���Ϊ0
	{
		//�ȼ���x���Ǹ�������
		size_t index_int = x / 32;
		//����x����һλ
		size_t index_bit = x % 32;

		_bits[index_int] &= (~(1 << index_bit));
	}

	bool Find(size_t x)//����x�Ƿ���λͼ�ϣ�trueΪ����
	{
		//�ȼ���x���Ǹ�������
		size_t index_int = x / 32;
		//����x����һλ
		size_t index_bit = x % 32;

		//�жϵ�index_bitΪ�Ƿ�Ϊ1
		return ((_bits[index_int] >> index_bit) & 1) == 1;
	}
private:
	vector<int>_bits;							//char[12]
};
