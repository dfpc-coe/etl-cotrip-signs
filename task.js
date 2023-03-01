import fs from 'fs';
import ETL from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
    console.log('ok - .env file loaded');
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static schema() {
        return {
            type: 'object',
            required: ['COTRIP_TOKEN'],
            properties: {
                'COTRIP_TOKEN': {
                    type: 'string',
                    description: 'API Token for CoTrip'
                },
                'DEBUG': {
                    type: 'boolean',
                    default: false,
                    description: 'Print GeoJSON Features in logs'
                }
            }
        };
    }

    async control() {
        const layer = await this.layer();

        const api = 'https://data.cotrip.org/';
        if (!layer.data.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.data.environment.COTRIP_TOKEN;

        const incidents = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of incidents`);
            const url = new URL('/api/v1/incidents', api);
            url.searchParams.append('apiKey', token);
            if (res) url.searchParams.append('offset', res.headers.get('next-offset'));

            res = await fetch(url);

            incidents.push(...(await res.json()).features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${incidents.length} incidents`);

        const features = [];
        for (const feature of incidents.map((incident) => {
            incident.id = incident.properties.id;
            incident.properties.remarks = incident.properties.travelerInformationMessage;
            incident.properties.callsign = incident.properties.type;
            return incident;
        })) {
            if (feature.geometry.type.startsWith('Multi')) {
                const feat = JSON.stringify(feature);
                const type = feature.geometry.type.replace('Multi', '');

                let i = 0;
                for (const coordinates of feature.geometry.coordinates) {
                    const new_feat = JSON.parse(feat);
                    new_feat.geometry = { type, coordinates };
                    new_feat.id = new_feat.id + '-' + i;
                    features.push(new_feat);
                    ++i;
                }
            } else {
                features.push(feature);
            }
        }

        const fc = {
            type: 'FeatureCollection',
            features: features
        };

        await this.submit(fc);
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema') {
        return Task.schema();
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
