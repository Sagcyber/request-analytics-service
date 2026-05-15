import http from 'k6/http';
import { check } from 'k6';

export const options = {
    vus: 100,
    iterations: 10000,
};

export default function () {

    const response = http.get(
        'http://host.docker.internal:8080/status'
    );

    check(response, {
        'status is 200': (r) => r.status === 200,
    });
}
