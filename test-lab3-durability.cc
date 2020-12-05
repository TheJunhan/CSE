#include <string>
#include "ydb_protocol.h"
#include "ydb_client.h"
#include <iostream>

using namespace std;

int main(int argc, char **argv) {
	string r;
	// cout << "调用了test-lab3-durability" << endl;
	if (argc <= 1) {
		printf("Usage: `%s <ydb_server_listen_port>` or `%s <ydb_server_listen_port>` RESTART\n", argv[0], argv[0]);
		return 0;
	}

	//ydb_client y = ydb_client(string(argv[1]));
	ydb_client y((string(argv[1])));
	// cout << "开始了" << endl;
	if (argc == 3 && string(argv[2]) == "RESTART") {
		// cout << "这里是重启的" << endl;
		y.transaction_begin();
		string r1 = y.get("a");
		string r2 = y.get("b");
		string r3 = y.get("0123456789abcdef0123456789ABCDEF");
		// cout << "得到的a为：" << r1 << "得到的b为：" << r2 << "得到的c为：" << r3 << endl;
		//cout << "r1:" << r1 << "_" << "r2:" << r2 << endl;
		y.transaction_commit();

		if (r1 == "100-50" && r2 == "200+50" && r3 == "fedcba9876543210FEDCBA9876543210") {
			printf("Equal\n");
		}
		else {
			printf("Diff\n");
		}

		return 0;
	}
	// cout << "没重启的开始：" << endl;
	y.transaction_begin();
	// y.set("c", "300");
	y.set("a", "100");
	y.set("b", "200");
	y.set("0123456789abcdef0123456789ABCDEF", "fedcba9876543210FEDCBA9876543210");
	y.transaction_commit();
	// cout << "没重启的到一半" << endl;
	y.transaction_begin();
	string tmp1 = y.get("a");
	string tmp2 = y.get("b");
	y.set("a", tmp1+"-50");
	y.set("b", tmp2+"+50");
	y.transaction_commit();
	
	/*
	y.transaction_begin();
	string r1 = y.get("a");
	string r2 = y.get("b");
	//cout << "r1: " << r1 << endl << "r2: " << r2 << endl;
	y.transaction_commit();
	*/
	
	return 0;
}

