set PATH=C:\dmd2\windows\bin;C:\Program Files\Microsoft SDKs\Windows\v7.0A\\bin;%PATH%

echo tango\core\BitManip.d >Debug\Pbus.build.rsp
echo tango\core\ByteSwap.d >>Debug\Pbus.build.rsp
echo tango\core\Exception.d >>Debug\Pbus.build.rsp
echo tango\math\random\Twister.d >>Debug\Pbus.build.rsp
echo tango\text\convert\Integer.d >>Debug\Pbus.build.rsp
echo tango\util\digest\Digest.d >>Debug\Pbus.build.rsp
echo tango\util\digest\Md4.d >>Debug\Pbus.build.rsp
echo tango\util\digest\Md5.d >>Debug\Pbus.build.rsp
echo tango\util\digest\MerkleDamgard.d >>Debug\Pbus.build.rsp
echo tango\util\digest\Sha01.d >>Debug\Pbus.build.rsp
echo tango\util\digest\Sha1.d >>Debug\Pbus.build.rsp
echo tango\util\uuid\NamespaceGenV3.d >>Debug\Pbus.build.rsp
echo tango\util\uuid\NamespaceGenV5.d >>Debug\Pbus.build.rsp
echo tango\util\uuid\RandomGen.d >>Debug\Pbus.build.rsp
echo tango\util\uuid\Uuid.d >>Debug\Pbus.build.rsp
echo dzmq.d >>Debug\Pbus.build.rsp
echo libzmq_headers.d >>Debug\Pbus.build.rsp
echo Logger.d >>Debug\Pbus.build.rsp
echo ppqueue.d >>Debug\Pbus.build.rsp
echo Worker.d >>Debug\Pbus.build.rsp

dmd -g -debug -X -Xf"Debug\Pbus.json" -of"Debug\Pbus.exe_cv" -deps="Debug\Pbus.dep" -map "Debug\Pbus.map" -L/NOMAP C:\work\Pbus\libs\libzmq.lib @Debug\Pbus.build.rsp
if errorlevel 1 goto reportError
if not exist "Debug\Pbus.exe_cv" (echo "Debug\Pbus.exe_cv" not created! && goto reportError)
echo Converting debug information...
"C:\Program Files\VisualD\cv2pdb\cv2pdb.exe" -D2 "Debug\Pbus.exe_cv" "Debug\Pbus.exe"
if errorlevel 1 goto reportError
if not exist "Debug\Pbus.exe" (echo "Debug\Pbus.exe" not created! && goto reportError)

goto noError

:reportError
echo Building Debug\Pbus.exe failed!

:noError