import sys
import pyspark_recommand_system
from UI import main_page_ui, recommand_page
from PyQt5.QtWidgets import *
from PyQt5 import uic


class WindowClass(QMainWindow) :
    def __init__(self):
        super().__init__()

        # 실행 시 Main UI 초기화
        self.setFixedSize(551, 381)
        self.ui = main_page_ui.Ui_MainWindow()
        self.ui.setupUi(self)

        # 실행 시 Recommand page 객체 생성 및 초기화, textEdit을 보내주는 이유는 돌아왔을때 clear 하기위하여
        self.recommand_page = recommand_page.newRecommandClass(self.ui.textEdit)

        # 실행 시 pyspark를 통해 movie data를 받아서 모델 생성
        self.recommand_system = pyspark_recommand_system.RecommandSystem()

        # 결과 변수 초기화
        self.result = None
        self.title_custom_top5_array = []
        self.genres_custom_top5_array = []

        self.result2 = None
        self.title_total_top5_array = []
        self.genres_total_top5_array = []

    # 메인 페이지에서 유저 입력 후 "search" 버튼을 눌렀을 때
    def search_clicked(self):

        # EditText에 적은 유저 id 가져옴
        user_id = int(self.ui.textEdit.toPlainText())

        # 해당 유저 ID로 커스텀 영화 추천 결과 받기
        self.result = self.recommand_system.top_movies(user_id, 5)

        # 추천된 커스텀 영화 title을 array에 저장
        title_list = self.result.select('title').collect()
        self.title_custom_top5_array = [row.title for row in title_list]

        # 추천된 커스텀 영화 genre를 array에 저장
        genres_list = self.result.select('genres').collect()
        self.genres_custom_top5_array = [row.genres for row in genres_list]

        # 전체 데이터에서 평점 높은 5개 영화 구하기
        self.result2 = self.recommand_system.top_total_movies()

        # 전체 평점에서 추천된 top5 영화 title을 array에 저장
        title_list2 = self.result2.select('title').collect()
        self.title_total_top5_array = [row.title for row in title_list2]

        # 전체 평점에서 추천된 top5 영화 genre를 array에 저장
        genres_list2 = self.result2.select('genres').collect()
        self.genres_total_top5_array = [row.genres for row in genres_list2]

        # Recommand Page로 넘어가기 전에 다음페이지의 id와 title, genre 표를 채우기 위하여 값 전달
        self.recommand_page.setId(user_id)
        self.recommand_page.setResult(self.title_custom_top5_array, self.genres_custom_top5_array)
        self.recommand_page.setResult2(self.title_total_top5_array, self.genres_total_top5_array)

        # Recommand Page 실행
        self.recommand_page.show()


if __name__ == "__main__" :
    #QApplication : 프로그램을 실행시켜주는 클래스
    app = QApplication(sys.argv)

    #WindowClass의 인스턴스 생성
    myWindow = WindowClass()

    #프로그램 화면을 보여주는 코드
    myWindow.show()

    #프로그램을 이벤트루프로 진입시키는(프로그램을 작동시키는) 코드
    app.exec_()