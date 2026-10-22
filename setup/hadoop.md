# Windows에서 Spark용 Hadoop(winutils) 설정

Windows에서 PySpark가 파일을 쓰려면 `winutils.exe`, `hadoop.dll`이 필요합니다.
바이너리는 저장소에 올리지 않고 출처와 설치 방법만 남깁니다.

- 버전: Hadoop 3.4.0 Windows x64 빌드
- 출처: https://github.com/kontext-tech/winutils (`hadoop-3.4.0-win10-x64/bin`), Apache License 2.0

## 설치 (sparkcamp STEP 03)

1. `hadoop` 폴더를 `C:\`에 복사해 `C:\hadoop\bin\winutils.exe`가 되게 합니다.
2. 환경 변수 `HADOOP_HOME = C:\hadoop`을 만듭니다.
3. `Path`에 `%HADOOP_HOME%\bin`을 추가합니다.
4. VS Code를 다시 켭니다.

함께 필요한 것: Java 17 (Temurin, 설치 시 `Set JAVA_HOME variable` 체크), Python 3.13.
자세한 순서는 [04_sparkcamp/시작하기.txt](../04_sparkcamp/시작하기.txt)를 보세요.
