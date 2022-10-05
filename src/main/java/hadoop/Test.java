package hadoop;

import java.io.*;

import hadoop.keyvalueClass.BookChapCharName;

public class Test {
    public static void main(String[] args) throws IOException {
//        // 路径
//        String path = "E:\\Uni\\work\\grade3.1\\distributed memory and parallel compute\\英文原版世界名著100本（TXT）\\20books";
//        File f = new File(path);
//
//        // 路径不存在
//        if(!f.exists()){
//            System.out.println(path + " not exists");
//            return;
//        }
//
//        File result[] = f.listFiles();
//        //  循环遍历
//        for(int i = 0; i<result.length; i++){
//            File fs = result[i];
//                System.out.println(fs.getName());
//        }
//
//        String t = "Agnes Grey-艾格妮斯·格雷,25\tAGNES GREY-C23.txt\tAshby\t1\tThomas,Harry,Murray,Mss,";
//        BookChapCharName b1 = new BookChapCharName();
//        b1.setBookName("A Christmas Carol-圣诞欢歌,5");
//        b1.setChapterNo("A CHRISTMAS CAROL-C03.txt");
//        b1.setCharacterName("!");
//
//        BookChapCharName b2 = new BookChapCharName();
//        b2.setBookName("A Christmas Carol-圣诞欢歌,5");
//        b2.setChapterNo("A CHRISTMAS CAROL-C03.txt");
//        b2.setCharacterName("!");
//
//        System.out.println(b1.compareTo(b2));

        String s = "a\nb\n";
        String[] ss = s.split("\n");
        System.out.println(ss[1]);


    }

}
