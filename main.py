def process_videointelligence(event, context):

    from fractions import Fraction
    from google.cloud import bigquery

    client = bigquery.Client()
    dataset_id = 'iot_video_dataset'
    table_id = 'detections'
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    if 'data' in event:
        top = float(event['attributes'].get('top'))
        bottom = float(event['attributes'].get('bottom'))
        left = float(event['attributes'].get('left'))
        right = float(event['attributes'].get('right'))
        zoom = int(event['attributes'].get('zoom'))
        row = (top,
               bottom,
               left,
               right,
               event['attributes'].get('entity_desc'),
               event['attributes'].get('entity_id'),
               float(event['attributes'].get('confidence')),
               event['attributes'].get('time'),
               int(event['attributes'].get('track_id')),
               zoom,
               )

    rows_to_insert = [
        row
    ]

    errors = client.insert_rows(table, rows_to_insert)  # insert to bq

    def send_command(top, bottom, left, right):
        from google.cloud import iot_v1

        project = 'iot-video-demo'
        region = 'us-central1'
        registry = 'esp32-registry'
        device = 'esp32_BCD168'

        iot_client = iot_v1.DeviceManagerClient()
        iot_name = iot_client.device_path(project, region, registry, device)

        right_pixel = round(right * 1920)
        left_pixel = round(left * 1920)
        top_pixel = round(top * 1080)
        bottom_pixel = round(bottom * 1080)

        len_x = (right_pixel - left_pixel)
        len_y = (bottom_pixel - top_pixel)
        center_x = round((left_pixel) + (len_x / 2))
        center_y = round((top_pixel) + (len_y / 2))

        z = max((right - left), (bottom - top))  # longer side to max zoom to
        zo = Fraction(z).limit_denominator()  # removes float weirdness
        zoo = zo.denominator * 100
        zoom = round(zoo / zo.numerator)

        command = f'{{"x":{center_x},"y":{center_y},"zoom":{zoom}}}'
        binary_data = command.encode('utf-8')

        response = iot_client.send_command_to_device(iot_name, binary_data)
        print(f'giraffe - {command} - l: {left_pixel} r: {right_pixel}
                                      t: {top_pixel} b: {bottom_pixel}')
        return response

    if zoom:
        zoomed = send_command(top, bottom, left, right)
