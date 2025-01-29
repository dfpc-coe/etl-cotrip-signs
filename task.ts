import { Static, Type, TSchema } from '@sinclair/typebox';
import { Feature, Geometry } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, InputFeatureCollection, DataFlowType, InvocationType } from '@tak-ps/etl';

export default class Task extends ETL {
    static name = 'etl-cotrip-signs';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Type.Object({
                    'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
                    'Point Geometries': Type.Boolean({ description: 'Allow point geometries', default: true }),
                    'LineString Geometries': Type.Boolean({ description: 'Allow LineString geometries', default: true }),
                    'Polygon Geometries': Type.Boolean({ description: 'Allow Polygon Geometries', default: true }),
                    'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false })
                });
            } else {
                return Type.Object({
                   communicationStatus: Type.String(),
                   marker: Type.Number(),
                   messageText: Type.String(),
                   direction: Type.String(),
                   lastUpdated: Type.String(),
                   messagePreview: Type.String(),
                   displayStatus: Type.String(),
                   name: Type.String(),
                   id: Type.String(),
                   speed: Type.Number(),
                   routeName: Type.String(),
                   messageMarkup: Type.String(),
                   publicName: Type.String(),
                   submittedBy: Type.String(),
                   nativeId: Type.String(),
                   activationTime: Type.String(),
                });
            }
        } else {
            return Type.Object({});
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        const api = 'https://data.cotrip.org/';
        if (!layer.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.environment.COTRIP_TOKEN;

        const signs = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of signs`);
            const url = new URL('/api/v1/signs', api);
            url.searchParams.append('apiKey', String(token));
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

        const allowed: string[] = [];
        if (layer.environment['Point Geometries']) allowed.push('Point');
        if (layer.environment['LineString Geometries']) allowed.push('LineString');
        if (layer.environment['Polygon Geometries']) allowed.push('Polygon');

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features.filter((feat) => {
                return allowed.includes(feat.geometry.type);
            })
        };

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}
