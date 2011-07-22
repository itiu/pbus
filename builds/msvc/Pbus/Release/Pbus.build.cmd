set PATH=C:\dmd2\windows\bin;C:\Program Files\Microsoft SDKs\Windows\v7.0A\\bin;%PATH%

echo tango\core\BitManip.d >Release\Pbus.build.rsp
echo tango\core\ByteSwap.d >>Release\Pbus.build.rsp
echo tango\core\Exception.d >>Release\Pbus.build.rsp
echo tango\text\convert\Integer.d >>Release\Pbus.build.rsp
echo tango\util\digest\Digest.d >>Release\Pbus.build.rsp
echo tango\util\digest\Md4.d >>Release\Pbus.build.rsp
echo tango\util\digest\Md5.d >>Release\Pbus.build.rsp
echo tango\util\digest\MerkleDamgard.d >>Release\Pbus.build.rsp
echo tango\util\digest\Sha01.d >>Release\Pbus.build.rsp
echo tango\util\digest\Sha1.d >>Release\Pbus.build.rsp
echo tango\util\uuid\NamespaceGenV3.d >>Release\Pbus.build.rsp
echo tango\util\uuid\NamespaceGenV5.d >>Release\Pbus.build.rsp
echo tango\util\uuid\RandomGen.d >>Release\Pbus.build.rsp
echo tango\util\uuid\Uuid.d >>Release\Pbus.build.rsp
echo tango\math\random\Twister.d >>Release\Pbus.build.rsp
echo dzmq.d >>Release\Pbus.build.rsp
echo libzmq_headers.d >>Release\Pbus.build.rsp
echo Logger.d >>Release\Pbus.build.rsp
echo ppqueue.d >>Release\Pbus.build.rsp
echo Worker.d >>Release\Pbus.build.rsp

dmd -release -X -Xf"Release\Pbus.json" -of"Release\Pbus.exe" -deps="Release\Pbus.dep" -map "Release\Pbus.map" -L/NOMAP @Release\Pbus.build.rsp
if errorlevel 1 goto reportError
if not exist "Release\Pbus.exe" (echo "Release\Pbus.exe" not created! && goto reportError)

goto noError

:reportError
echo Building Release\Pbus.exe failed!

:noError
