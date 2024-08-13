#ifndef ____UTILS_H_____
#define ____UTILS_H_____

//forum said to print to stdout
// https://moodle2.cs.huji.ac.il/nu22/mod/forum/discuss.php?d=79558#p129002
#define SYSCALL_FAIL(str_msg) std::cout << "system error: " <<__func__<<":"<<__LINE__<<" "<< str_msg << "\n"
#define EXIT_IF(predicate, msg) if (predicate){ SYSCALL_FAIL(msg);exit(1);}

#define CLEAN_AND_EXIT_IF(predicate, msg)if (predicate){ SYSCALL_FAIL(msg); clean();exit(1);}


#endif // ____UTILS_H_____