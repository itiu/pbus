DMD=dmd

VERSION_MAJOR=0
VERSION_MINOR=1
VERSION_PATCH=1

cp -v -r src/* build/src

git log -1 --pretty=format:"module myversion; public static string major=\"$VERSION_MAJOR\"; public static string minor=\"$VERSION_MINOR\"; public static string patch=\"$VERSION_PATCH\"; public static string author=\"%an\"; public static string date=\"%ad\"; public static string hash=\"%h\";">src/myversion.d

rm PBUS-$VERSION_MAJOR-$VERSION_MINOR-$VERSION_PATCH-32
rm *.log
rm *.io
rm *.oi

$DMD -m32 -debug -d \
@dfiles-linux -ofPBUS-$VERSION_MAJOR-$VERSION_MINOR-$VERSION_PATCH-32
rm *.o

