import fs from 'fs';
import ETL from '@tak-ps/etl';
import moment from 'moment-timezone';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
    console.log('ok - .env file loaded');
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static async schema(type = 'input') {
        if (type === 'input') {
            return {
                type: 'object',
                required: ['COTRIP_TOKEN'],
                properties: {
                    'COTRIP_TOKEN': {
                        type: 'string',
                        description: 'API Token for CoTrip'
                    },
                    'Point Geometries': {
                        type: 'boolean',
                        description: 'Allow point geometries'
                    },
                    'LineString Geometries': {
                        type: 'boolean',
                        description: 'Allow LineString geometries'
                    },
                    'Polygon Geometries': {
                        type: 'boolean',
                        description: 'Allow Polygon Geometries'
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print GeoJSON Features in logs'
                    }
                }
            };
        } else {
            return {
                type: 'object',
                required: [],
                properties: {
                   communicationStatus: { type: "string" },
                   marker: { type: "number" }
                   messageText: { type: 'string' },
                   direction: { type: 'string' },
                   lastUpdated: { type: 'string' },
                   messagePreview: { type: 'string' },
                   displayStatus: { type: 'string' },
                   name: { type: 'string' },
                   id: { type: 'string' },
                   speed: { type: 'number' },
                   routeName: { type: 'string' },
                   messageMarkup: { type: 'string' },
                   publicName: { type: 'string' },
                   submittedBy: { type: 'string' },
                   nativeId: { type: 'string' },
                   activationTime: { type: 'string' },
                }
            };
        }
    }

    async control() {
        const layer = await this.layer();

        const api = 'https://data.cotrip.org/';
        if (!layer.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.environment.COTRIP_TOKEN;

        const signs = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of signs`);
            const url = new URL('/api/v1/signs', api);
            url.searchParams.append('apiKey', token);
            if (res) url.searchParams.append('offset', res.headers.get('next-offset'));

            res = await fetch(url);

            signs.push(...(await res.json()).features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${signs.length} signs`);

        const features = [];
        for (const feature of signs.map((sign) => {
            console.error(sign)
            return {
                id: sign.properties.id,
                type: 'Feature',
                properties: {
                },
                geometry: sign.geometry
            };
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

        const allowed = [];
        if (layer.environment['Point Geometries']) allowed.push('Point');
        if (layer.environment['LineString Geometries']) allowed.push('LineString');
        if (layer.environment['Polygon Geometries']) allowed.push('Polygon');

        const fc = {
            type: 'FeatureCollection',
            features: features.filter((feat) => {
                return allowed.includes(feat.geometry.type);
            })
        };

        //await this.submit(fc);
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema('input');
    } else if (event.type === 'schema:output') {
        return await Task.schema('output');
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
