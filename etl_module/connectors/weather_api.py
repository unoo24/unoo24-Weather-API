import requests


class WeatherApiClient:
    """
    OpenWeatherMap API 클라이언트를 사용하여 날씨 데이터를 가져오는 클래스입니다.
    """

    def __init__(self, api_key: str):
        self.base_url = "http://api.openweathermap.org/data/2.5"
        if api_key is None:
            raise Exception("API 키는 None으로 설정할 수 없습니다.")
        self.api_key = api_key

    def get_city(self, city_name: str, temperature_units: str = "metric") -> dict:
        """
        지정된 도시의 최신 날씨 데이터를 가져옵니다.

        Parameters:
        - city_name (str): 날씨 정보를 조회할 도시 이름.
        - temperature_units (str): 온도 단위 (기본값은 'metric'으로 섭씨 기준).
                                   'metric'은 섭씨, 'imperial'은 화씨, 'standard'는 켈빈 단위를 의미합니다.

        Returns:
        - dict: 요청한 도시의 날씨 데이터가 포함된 JSON 응답을 반환합니다.

        Raises:
        - Exception: API 요청이 실패한 경우 상태 코드와 응답 메시지와 함께 예외가 발생합니다.
        """
        params = {"q": city_name, "units": temperature_units, "appid": self.api_key}
        response = requests.get(f"{self.base_url}/weather", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Open Weather API에서 데이터를 추출하지 못했습니다. 상태 코드: {response.status_code}. 응답: {response.text}"
            )
